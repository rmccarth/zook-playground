#the goal of this file will be to poll the zookeeper repo
#and find new topics and subscribe to them to get their updates
from kazoo.client import KazooClient
import zmq
import logging.handlers

#logging configuration
LOG_FILENAME = "subscriber.log"
subscribe_logger = logging.getLogger("MyLogger")
subscribe_logger.setLevel(logging.DEBUG)
handler = logging.handlers.RotatingFileHandler(
              LOG_FILENAME, maxBytes=20, backupCount=5)
subscribe_logger.addHandler(handler)

#setup our client to communicate with zookeeper
zk = KazooClient(hosts="127.0.0.1:2182")
zk.start()

#a function that listens for new topics (node names) published to / of the zookeeper filesystem
available_topics = zk.get_children("/")
def watcher():
    #this is a callback register w/zookeeper to be informed of updates to the root node
    #where we are advertising topic names
    @zk.ChildrenWatch("/")
    def getTopic(children):
        
        global available_topics

        for child in children:
            if child not in available_topics:
                available_topics.append(child)
                print("NEW AVAILABLE TOPIC: " + child)
                subscribe_logger.debug("NEW AVAILABLE TOPIC: " + child)
                subscribe(child)

def subscribe(topic):
    context = zmq.Context() #create the context to pass to the client
    sock = context.socket(zmq.SUB)
    sock.setsockopt_string(zmq.SUBSCRIBE, topic)
    sock.connect("tcp://127.0.0.1:5555")
    
    subscribe_logger.debug("Writing Data Stream to subscription-output.txt")
    with open('subscription-output.txt', 'a') as f:
        f.write(sock.recv().decode('UTF-8') + "\n")
        f.close()
    subscribe_logger.debug("Completed Writing Data Stream")

watcher()
while True:
    pass

