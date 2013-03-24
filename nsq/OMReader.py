"""

One More Reader.

high-level NSQ reader class built on top of a select.poll() supporting async (callback) mode of operation.

Supports multiple nsqd connections using plain list of hosts, multiple A-records or (near future) SRV-requests.

Differences from NSQReader:
1. Message processing assume to be independent from message to message. One failed message do not make slower overall
   message processing (not using BackoffTimer at all).
2. We don't use lookupd server query since all required nsqd may be defined through DNS.
3. Max message attempts and requeue interval is controlled by user-side (at callback function). The only requeue
   delay is for failed callbacks.
4. OMReader do not use tornado. It uses select.poll() object for handling multiple async connections.
5. As of 5, all OMReader instances are isolated. You can run and stop them as you wish.

Example:

```
    total = 0
    def my_callback(message):
        global total, reader
        
        total += 1
        
        if total >= 1000:
            reader.stop()

        # requeue every third message just for fun
        if random.choice([True, False, True]):
            message.finish()
        else:
            message.requeue(5)

    reader = OMReader([('nsqd.example.com', 4150)], my_callback, 'test', max_in_flight=1000)
    
    reader.run()
```

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
import struct

import nsq

def resolve_nsqd_addresses(hostports):
    """Resolve addresses to host-port tuples (compatible with socket.connect()), check uniqueness and return it"""
    addresses = []

    for host, port in hostports:
        try:
            logging.debug("Resolving address %s:%s (tcp, ipv4)", host, port)
            gai = socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)
        except:
            logging.error("Error resolving address %s:%s: %s", host, port, sys.exc_info()[1])
            gai = []

        for family, socktype, proto, canonname, sockaddr in gai:
            if not sockaddr in addresses:
                logging.debug("Adding %s to list of known nsqd servers", sockaddr)
                addresses.append(sockaddr)

    return addresses


class OMMessage(nsq.Message):
    """Extended message object. Can handle finalize/requeue events by itself"""

    def __init__(self, message, socket):
        super(OMMessage, self).__init__(message.id, message.body, message.timestamp, message.attempts)
        self.socket = socket
        self.status = 'pending'
        
    def finish(self):
        """Finalize message (mark as successfull processed)"""
        assert self.status == 'pending', "Message is already finalized (status: %s)" % self.status
        logging.debug("Sending FIN for %s", self.id)
        self.socket.sendall(nsq.finish(self.id))
        self.status = 'finished'
        
        
    def requeue(self, delay=60):
        """Requeue message with delay (in seconds)"""
        assert self.status == 'pending', "Message is already finalized (status: %s)" % self.status
        logging.debug("Sending REQ for %s", self.id)
        self.socket.sendall(nsq.requeue(self.id, str(int(delay * 1000))))
        self.status = 'requeued'
        
        
class OMReader(object):
    def __init__(self, nsqd_addresses, message_callback, topic, channel=None, max_in_flight=1, requeue_delay=90):
        """
Initializes OMReader object.

Required arguments:
  * nsqd_addresses - tuple of (host, port) addresses, compatible with socket.getaddrinfo() call. If returned more
  than one - connections will made to every returned record
  * message_callback receives one argument - OMMessage object. Callback function must finalize message by itself
  (using .finish() or .requeue(delay) methods). If callback have not finalized message, OMReader will requeue
  this message with default delay (requeue_delay argument). OMReader ignores (but logs) possible message_callbacks
  exceptions. Only message.status is counted
  * topic - nsqd topic to listen

Optional arguments:
  * channel - channel to connect. Defaults to topic name
  * max_in_flight - will be uses in RDY command for every connection (may be changed in future versions)
  * requeue_delay - default requeue delay if message_callback fail (raises exception)

        """
        
        self.message_callback = message_callback
        self.topic = topic
        self.channel = channel or topic
        self.max_in_flight = max_in_flight
        self.requeue_delay = requeue_delay
        
        self.nsqd_addresses = nsqd_addresses # just store what was passed to constructor
        self.nsqd_tcp_addresses = resolve_nsqd_addresses(self.nsqd_addresses)
        self.poll = select.poll()
        self.shutdown = False

        self.hostname = socket.gethostname()
        self.short_hostname = self.hostname.split('.')[0]
        
        
    def connect(self):
        self.connections = {}
        self.connections_last_check = time.time()

        if self.nsqd_tcp_addresses:
            for address in self.nsqd_tcp_addresses:
                self.connect_one(address)
        else:
            raise Exception("No connections available. Can't run!")
        
    def connect_one(self, address):
        logging.debug("Connecting to %s", address)
        
        self.connections[address] = {
            'socket': None,
            'state': 'connecting',
            'time': time.time(),
            #'out': '', # output buffer
            'in': '', # incoming buffer
            'closed': True,
        }
        
        try:
            sock = socket.create_connection(address, 5)
        except socket.error:
            logging.error("Error connecting: %s" % sys.exc_info()[1])
            return None

        self.connections[address]['socket'] = sock
        self.connections[address]['state'] = 'connected'
        self.connections[address]['closed'] = False
        
        sock.setblocking(0)
        
        sock.send(nsq.MAGIC_V2)
        sock.send(nsq.subscribe(self.topic, self.channel, self.short_hostname, self.hostname))
        if not self.shutdown:
            logging.debug("Sending max_in_flight=%s to %s", self.max_in_flight, address)
            sock.send(nsq.ready(self.max_in_flight))

        # maybe, poll POLLOUT as well, but it is so boring
        self.poll.register(sock, select.POLLIN + select.POLLPRI + select.POLLERR + select.POLLHUP)
        

    def connections_check(self):
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

    def connection_search(self, fd):
        item = None
        for address, item in self.connections.items():
            if item['socket'] and item['socket'].fileno() == fd:
                break
        
        return address, item

            

    def connection_close(self, address):
        logging.debug("unregistering and closing connection to %s", address)
        
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
            packet_length = struct.unpack('>l', data[:4])[0]
            if len(data) >= 4 + packet_length:
                # yeah, complete response received

                frame_id, frame_data = nsq.unpack_response(data[4 : packet_length + 4])
                if frame_id == nsq.FRAME_TYPE_MESSAGE:
                    
                    message = OMMessage(nsq.decode_message(frame_data), self.connections[address]['socket'])

                    logging.debug("Calling callback")
                    self.message_callback(message)
                    logging.debug("Callback done. Message status == %s", message.status)
                    
                    if message.status == 'pending':
                        logging.warning("Message not processed. Requeueing message %s with delay %d", message.id, self.requeue_delay)
                        message.requeue(self.requeue_delay)
                    
                    if not self.shutdown:
                        logging.debug("Sending max_in_flight=%d to %s", self.max_in_flight, address)
                        self.connections[address]['socket'].send(nsq.ready(self.max_in_flight))
                    
                    
                    
                elif frame_id == nsq.FRAME_TYPE_ERROR:
                    logging.error("Error received. Frame data: %s", repr(frame_data))
                    
                elif frame_id == nsq.FRAME_TYPE_RESPONSE:
                    if frame_data == '_heartbeat_':
                        logging.debug("hearbeat received. Answering with nop")
                        self.connections[address]['socket'].send(nsq.nop())
                    else:
                        logging.debug("Unknown response received. Frame data: %s", repr(frame_data))
                else:
                    logging.error("Unknown frame_id received: %s. Frame data: %s", frame_id, repr(frame_data))
                
                
                res = data[4 + packet_length : ]
                
        return res
    
    def stop(self):
        logging.info("Shutdown requested")
        self.shutdown = True
        for address, item in self.connections.items():
            logging.debug("Sending CLS to %s", address)
            item['socket'].sendall(nsq.cls())

        
    def run(self):
        
        self.connect()

        logging.info("Starting OMReader for topic '%s'..." % self.topic)
        
        while True:
            time.sleep(0.00001)
            data = self.poll.poll(1)

            if not data and self.shutdown:
                logging.info("No more data in sockets. Shutting down")
                break
            
            closed_addresses = []
            
            for fd, event in data:
                
                address, item = self.connection_search(fd)
                if not item:
                    continue
                
                
                if event & select.POLLIN or event & select.POLLPRI:
                    logging.debug("%s: There are data to read", address)
                    data = item['socket'].recv(8192)
                    if data:
                        item['in'] += data
                        
                        while len(item['in']) > 4:
                            old_len = len(item['in'])
                            data_new = self.process_data(address, item['in'])
                            
                            if not data_new is None:
                                item['in'] = data_new
                            
                            if data_new is None or old_len == len(data_new):
                                # data not changed, breaking loop
                                break
                            
                        
                    else:
                        logging.warning("%s Socket closed", address)
                        if not address in closed_addresses:
                            closed_addresses.append(address)

                    
                ## elif event & select.POLLOUT:
                ##     if item['out']:
                ##         logging.(print "%s: Can write data there" % (address,)
                ##         item['socket'].send(item['out'])
                ##         item['out'] = ''
                
                elif event & select.POLLHUP or event & select.POLLERR:
                    logging.warning("%s: Socket failed. Need reconnecting", address)
                    
                    if not address in closed_addresses:
                        closed_addresses.append(address)

            for address in closed_addresses:
                self.connection_close(address)
            
            
            if time.time() - self.connections_last_check > 5:
                self.connections_check()
            
            
        
    
        

if __name__ == '__main__':

    logging.basicConfig(level=logging.DEBUG)

    total = 0
    def test_callback(message):
        global total, reader
        logging.info("Received message id: %s, timestamp: %s, attempts: %d, body: %s", message.id, message.timestamp, message.attempts, repr(message.body))
        
        total += 1
        
        logging.info("Total: %d", total)

        if total >= 1000:
            reader.stop()

        if random.choice([True, False, True]):
            message.finish()
        else:
            message.requeue(5)
    
    

    if len(sys.argv) > 2:
        host, port = sys.argv[2].split(':')
        addresses = [(host, int(port))]
    else:
        addresses = [('localhost', 4150)]
        
    reader = OMReader(addresses, test_callback, sys.argv[1], max_in_flight=1000)
    
    reader.run()

    
            
