#this file advertises two topics in zookeeper, and then publishes data to
#them using zeroMQ. 
#think of zookeeper as a message board advertising a sign-up email address for a 
#d-list. You get the d-list email from the message board, and receive notifications
#from the d-list.

from kazoo.client import KazooClient
import zmq
import time
import sys
import logging.handlers

#logging configuration
LOG_FILENAME = "publisher.log"
publish_logger = logging.getLogger("MyLogger")
publish_logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler(
              LOG_FILENAME, maxBytes=5000, backupCount=5)
publish_logger.addHandler(handler)

context = zmq.Context()
sock = context.socket(zmq.PUB)
sock.bind("tcp://127.0.0.1:5555") #define a port on which to setup our zeroMQ publisher

def main(topic):
    publish_logger.debug("received topic: " + topic)

    #the topics defined here are of arbitrary name and content type
    #this could be extended to publish much more data, or mission-realistic data if required.
    if topic == "genres":
        
        topic = "genres"
        genres = ['historical-fiction', 'biography', 'fiction', 'non-fiction', 'science-fiction', \
            'horror', 'fantasy', 'romance']
        #inform the public of a new topic before publishing
        inform_zookeeper(topic)
        #publish the content to the zeroMQ topic
        publish_zmq(topic, genres)

    if topic == "animals":
        topic = "animals"
        species = ["dogs", "cats", "mammals", "birds", "reptiles"]
        #inform the public of a new topic before publishing
        inform_zookeeper(topic)

        publish_zmq(topic, species)
    
    else:
        print("NO DATASET FOR SPECIFIED TOPIC")


def publish_zmq(topic, messages):
    publish_logger.debug("publish_zmq | topic: " + topic + " | message: " + str(messages))

    for i in range (10):
        for message in messages:
            time.sleep(3)
            print("sending " + topic + " + " + message)

            sock.send_string(topic + " " + message)
            time.sleep(3)

def inform_zookeeper(topic):

    zk = KazooClient(hosts="127.0.0.1:2182")
    zk.start()
    publish_logger.debug("nodes contained in zookeeper root directory: " + str(zk.get_children("/")))

    topic_name = "/" + topic
    if topic not in zk.get_children("/"):
        zk.create(topic_name, b'127.0.0.1:2182')
    zk.stop()
    zk.close()

main("animals")
main("genres")

