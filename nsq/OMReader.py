"""

One More Reader.

high-level NSQ reader class built on top of a select.poll() supporting async (callback) mode of operation.

Supports multiple nsqd connections using plain list of hosts, multiple A-records or (near future) SRV-requests.

Differences from NSQReader:
1. Message processing assume to be independent from message to message. One failed message do not make slower overall message processing (not using BackoffTimer at all).
2. We don't use lookupd server query since all required nsqd may be defined through DNS.
3. Max message attempts and requeue interval is controlled by user-side. The only requeue delay is for failed callbacks
4. OMReader do not use tornado. It uses select.poll() object for handling multiple asyn connections
5. As of 5, all OMReader instances are isolated. You can run and stop them as you wish.

"""

import time
import select
import socket
import random
import nsq
import async
import logging
import sys
import os
from pprint import pprint
import struct

import nsq

class OMReader(object):
    def __init__(self, message_callback, topic, channel=None, nsqd_addresses=None, max_in_flight=1, requeue_delay=90):
        
        self.message_callback = message_callback
        self.topic = topic
        self.channel = channel or topic
        self.max_in_flight = max_in_flight
        self.requeue_delay = requeue_delay
        
        self.nsqd_addresses = nsqd_addresses
        self.nsqd_tcp_addresses = nsqd_addresses
        self.poll = select.poll()
        self.shutdown = False
        
    def resolve_nsqd_addresses(self):
        pass


    def connect(self):
        self.connections = {}
        self.connections_last_check = time.time()
        
        for address in self.nsqd_tcp_addresses:
            self.connect_one(address)

    def connect_one(self, address):
        print "Connecting to %s" % (address,)
        
        self.connections[address] = {
            'socket': None,
            'state': 'connecting',
            'time': time.time(),
            'out': '', # output buffer
            'in': '', # incoming buffer
            'closed': True,
        }
        
        try:
            sock = socket.create_connection(address, 5)
        except socket.error:
            print "Error connecting: %s" % sys.exc_info()[1]
            return None

        self.connections[address]['socket'] = sock
        self.connections[address]['state'] = 'connected'
        self.connections[address]['closed'] = False
        
        sock.setblocking(0)
        
        sock.send(nsq.MAGIC_V2)
        sock.send(nsq.subscribe(self.topic, self.channel, 'a', 'b'))
        sock.send(nsq.ready(10))
        
        self.poll.register(sock, select.POLLIN + select.POLLPRI + select.POLLOUT + select.POLLERR + select.POLLHUP)
        

    def connections_check(self):
        #print 'connections check'
        to_connect = []
        
        for address, item in self.connections.items():
            if (item['state'] == 'disconnected' or item['state'] == 'connecting') and (time.time() - item['time']) > 5:
                to_connect.append(address)
        
        
        
        for address in to_connect:
            if not self.connections[address]['closed']:
                self.connection_close(address)
            del(self.connections[address])

        for address in to_connect:
            self.connect_one(address)

        self.connections_last_check = time.time()
        #pprint(self.connections)

    def connection_search(self, fd):
        item = None
        for address, item in self.connections.items():
            if item['socket'] and item['socket'].fileno() == fd:
                break
        
        return address, item

            

    def connection_close(self, address):
        print 'unregistering and closing connection to %s' % (address,)
        
        item = self.connections[address]
        self.poll.unregister(item['socket'])
        
        item['socket'].close()
        item['state'] = 'disconnected'
        item['closed'] = True
        item['time'] = time.time()
        

    def process_data(self, address, data):
        """Process incoming data. All return data will remain as rest of buffer"""
        res = None
        if len(data) >= 4:
            print "Processing data"
            packet_length = struct.unpack('>l', data[:4])[0]
            if len(data) >= 4 + packet_length:
                # yeah, complete response received

                frame_id, frame_data = nsq.unpack_response(data[4 : packet_length + 4])
                if frame_id == nsq.FRAME_TYPE_MESSAGE:
                    self.connections[address]['socket'].sendall(nsq.ready(10))
                    message = nsq.decode_message(frame_data)
                    
                    try:
                        res, delay = self.message_callback(message)
                    except:
                        print "Error calling message callback. Requeuing message with default queue delay. Exception: %s" % sys.exc_info()[1]
                        res, delay = False, self.requeue_delay

                    if res:
                        print "Message %s processed successfully" % message.id
                        self.connections[address]['socket'].sendall(nsq.finish(message.id))
                    else:
                        print "Requeueing message %s with delay %d" % (message.id, delay)
                        self.connections[address]['socket'].sendall(nsq.requeue(message.id, str(int(delay * 1000))))
                    
                elif frame_id == nsq.FRAME_TYPE_ERROR:
                    print "error received. wtf?"
                    pprint(frame_data)
                elif frame_id == nsq.FRAME_TYPE_RESPONSE:
                    if frame_data == '_heartbeat_':
                        print "hearbeat received. Answering with nop"
                        self.connections[address]['socket'].send(nsq.nop())
                    else:
                        print "response received. wtf?"
                        pprint(frame_data)
                else:
                    print "unknown frame_id received: %s" % frame_id
                    pprint(frame_data)
                
                
                res = data[4 + packet_length : ]
                
        return res

    def stop(self):
        self.shutdown = True
        for address, item in self.connections.items():
            print "Sending CLS to %s" % (address,)
            item['socket'].sendall(nsq.cls())

        
    def run(self):
        
        self.connect()

        
        while not self.shutdown:
            #print "Polling"
            time.sleep(0.00001)
            data = self.poll.poll(1)
            
            closed_addresses = []
            
            for fd, event in data:
                
                address, item = self.connection_search(fd)
                if not item:
                    continue
                
                
                #print "Event received for connection #%d %s: %d" % (fd, self.connections[fd]['address'], event)
                
                if event & select.POLLIN or event & select.POLLPRI:
                    print "%s: There are data to read" % (address,)
                    data = item['socket'].recv(8192)
                    if data:
                        item['in'] += data
                        
                        while len(item['in']) > 4:
                            print '%s: buf len: %d' % (address, len(item['in']))
                            old_len = len(item['in'])
                            data_new = self.process_data(address, item['in'])
                            
                            if not data_new is None:
                                item['in'] = data_new
                            
                            if data_new is None or old_len == len(data_new):
                                # data not changed, breaking loop
                                print '%s: breaking loop, %s, %s' % (address, old_len, len(data_new))
                                break
                            
                        
                    else:
                        print "%s Socket closed" % (address,)
                        if not address in closed_addresses:
                            closed_addresses.append(address)

                    
                elif event & select.POLLOUT:
                    if item['out']:
                        print "%s: Can write data there" % (address,)
                        item['socket'].send(item['out'])
                        item['out'] = ''
                        
                elif event & select.POLLHUP or event & select.POLLERR:
                    print "%s: Socket failed. Need reconnecting" % (address,)
                    
                    if not address in closed_addresses:
                        closed_addresses.append(address)

            for address in closed_addresses:
                self.connection_close(address)
            
            
            if time.time() - self.connections_last_check > 5:
                self.connections_check()
            
            
        logging.info("Starting OMReader for topic '%s'..." % self.topic)
        pass
    
        

if __name__ == '__main__':

    total = 0
    def test_callback(message):
        global total, reader
        print("Received message id: %s, timestamp: %s, attempts: %d, body: %s" % (message.id, message.timestamp, message.attempts, message.body))
        
        total += 1
        
        print "*" * 50
        print "Total: %d" % total

        if total >= 1000:
            reader.stop()
        
        return random.choice([True, False, True]), 5
    

    addresses = [('conan', 4150), ('barlog', 4150)]
    addresses = [('localhost', 4150)]
    reader = OMReader(test_callback, sys.argv[1], sys.argv[1], addresses)
    
    reader.run()

    
            
