"""Simple http writer


"""
import sys
import json
import random
import time
import urllib

from OMReader import resolve_nsqd_addresses

def random_float(_from, to):
    return random.randrange(_from, to) + random.random()

def nsq_write(nsqd_addresses, topic, message, message_format="json", max_attempts=10):
    """Put message (optionally raw, defaults = json-encoded) with topic to nsqd-server (one of nsqds records).
  * nsqd_addresses is tuple of (host, nsqd_http_port) addresses, compatible with socket.getaddrinfo() call.
  If returned more than one - write will be made to one of them (first success)
  

    """
    
    if message_format == "raw":
        message_raw = message
    elif message_format == "json":
        message_raw = json.dumps(message, ensure_ascii=False).encode('utf-8')
    else:
        raise Exception("Unknown message encoding specified: %s" % message_format)

    
    addresses = resolve_nsqd_addresses(nsqd_addresses)
    
    if not addresses:
        raise Exception("No NSQD server defined")

    put_attempts = 0
    put = False
    
    while put_attempts < max_attempts:
        
        host, port = random.choice(addresses)

        try:
            response = urllib.urlopen('http://%s:%s/put?topic=%s' % (host, port, urllib.quote(topic)), message_raw).read()
            assert response == 'OK', "Invalid put response: %s" % response
            put = True
        except:
            print sys.exc_info()
            pass

        if put:
            break
        
        time.sleep(random_float(1, 5))
        put_attempts += 1

    
    return put


if __name__ == '__main__':


    if len(sys.argv) > 2:
        host, port = sys.argv[2].split(':')
        addresses = [(host, int(port))]
    else:
        addresses = [('localhost', 4151)]

    
    for i in range(0, 1000):
        nsq_write(addresses, sys.argv[1], {'text': 'Hello, world!', 'i': i})
    


    
