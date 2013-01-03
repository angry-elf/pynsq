"""Simple http writer


"""
import sys
import json
import random
import time
import urllib
import logging

from OMReader import resolve_nsqd_addresses

def random_float(_from, to):
    return random.randrange(_from, to) + random.random()

def nsq_write(nsqd_addresses, topic, message, message_format="json", max_attempts=10):
    """Put message (optionally raw, defaults = json-encoded) with topic to nsqd-server (one of nsqds records).
  * nsqd_addresses is tuple of (host, nsqd_http_port (usually, 4151)) addresses, compatible with socket.getaddrinfo() call.
  If returned more than one - write will be made to one of them (first success in random order)

Example:

>>> from nsq.writer import nsq_write
>>> nsq_write([('localhost', 4151)], 'test', 'Hello, world')
True


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
        logging.debug("Putting message to %s (attempt: %d/%d)", topic, put_attempts, max_attempts)
        host, port = random.choice(addresses)

        url = 'http://%s:%s/put?topic=%s' % (host, port, urllib.quote(topic))
        try:
            response = urllib.urlopen(url, message_raw).read()
            assert response == 'OK', "Invalid put response: %s" % response
            put = True
            logging.info("Message posted successfully")
        except:
            logging.error("Error POST'ing to %s: %s", url, sys.exc_info()[1])
            pass

        if put:
            break

        delay = random_float(1, 5)
        logging.debug("Sleeping %.3f seconds before retry", delay)
        time.sleep(delay)
        put_attempts += 1

    if not put:
        logging.warning("Message dropped - can't put to any server now")
    return put


if __name__ == '__main__':
    
    logging.basicConfig(level=logging.DEBUG)
    
    if len(sys.argv) > 2:
        host, port = sys.argv[2].split(':')
        addresses = [(host, int(port))]
    else:
        addresses = [('localhost', 4151)]

    
    for i in range(0, 1000):
        logging.info("Posting message #%d", i)
        result = nsq_write(addresses, sys.argv[1], {'text': 'Hello, world!', 'i': i})
        logging.info("Posting result: %s", result)
    


    
