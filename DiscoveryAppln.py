import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
from topic_selector import TopicSelector
from CS6381_MW.DiscoveryMW import DiscoveryMW
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2
from ChordDHT import Pingu
from enum import Enum  # for an enumeration we are using to describe what state we are in
from functools import wraps

class DiscoveryAppln(Pingu):
    class State(Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        COMPLETED = 3
    
    def handle_exception(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                raise e
        return wrapper
    
    def __init__(self, logger):
        super().__init__(logger) # call the parent class constructor for Chord DHT
        self.state = self.State.INITIALIZE # state that are we in
        self.name = None # our name (some unique name)
        self.topiclist = None # the different topics in the discovery service
        self.iters = None   # number of iterations of publication
        self.frequency = None # rate at which dissemination takes place
        self.num_topics = None # total num of topics in the discovery service
        self.no_pubs = 0 # Initialise to 0
        self.no_subs = 0 # Initialise to 0
        self.no_current_pubs = 0 # Initialise to 0
        self.no_current_subs = 0 # Initialise to 0
        self.no_broker = 0 # Initialise to 0
        self.pub_list = [] # Initialise to empty list
        self.sub_list = [] # Initialise to empty list
        self.broker_list = [] # Initalise to empty list
        self.lookup = None # one of the diff ways we do lookup
        self.dissemination = None # direct or via broker
        self.is_ready = False
    
    @handle_exception
    def configure(self, args):
        self.logger.info("DiscoveryAppln::configure")
        self.state = self.State.CONFIGURE
        self.name = args.name # our name
        self.iters = args.iters  # num of iterations
        self.frequency = args.frequency # frequency with which topics are disseminated
        self.num_topics = args.num_topics  # total num of topics we publish
        self.no_pubs = args.no_pubs
        self.no_subs = args.no_subs
        self.no_broker = args.no_broker
        config = configparser.ConfigParser()
        config.read(args.config)
        self.lookup = config["Discovery"]["Strategy"]
        self.dissemination = config["Dissemination"]["Strategy"]
        ip, port = super().readDhtJson(args)
        self.mw_obj.configure(ip, port, self.fingerTable, self.hashDict) # pass remainder of the args to the m/w object
        self.logger.info("DiscoveryAppln::configure - configuration complete")
    
    @handle_exception
    def driver(self):
        self.logger.info("DiscoveryAppln::driver")
        self.dump()
        self.logger.info("DiscoveryAppln::driver - upcall handle")
        self.mw_obj.set_upcall_handle(self)
        self.state = self.State.REGISTER
        self.mw_obj.event_loop(timeout=0)  # start the event loop
        self.logger.info("DiscoveryAppln::driver completed")

    @handle_exception    
    def register_request(self, reg_request):
        self.logger.info("DiscoveryAppln::register_request")
        reason = ""
        status = False
        if reg_request.role == discovery_pb2.ROLE_PUBLISHER:
            self.logger.info("DiscoveryAppln::register_request - ROLE_PUBLISHER")
            for pub in self.pub_list:
                if pub[0] == reg_request.info.id:
                    reason = "The publisher name is not unique."
            if reason == "":
                self.no_current_pubs += 1
                self.pub_list.append([reg_request.info.id, reg_request.info.addr, reg_request.info.port, reg_request.topiclist])
                status = True
                reason = "The publisher name is unique."
        elif reg_request.role == discovery_pb2.ROLE_SUBSCRIBER:
            self.logger.info("DiscoveryAppln::register_request - ROLE_SUBSCRIBER")
            for sub in self.sub_list:
                if sub[0] == reg_request.info.id:
                    reason = "The subscriber name is not unique."
            if reason == "":
                self.no_current_subs += 1
                self.sub_list.append([reg_request.info.id, reg_request.info.addr, reg_request.info.port, reg_request.topiclist])
                status = True
                reason = "The subscriber name is unique."
        elif reg_request.role == discovery_pb2.ROLE_BOTH:
            self.logger.info("DiscoveryAppln::register_request - ROLE_BOTH")
            if len(self.broker_list) != 0:
                reason = "There should be only one broker."
            if reason == "":
                self.broker_list.append([reg_request.info.id, reg_request.info.addr, reg_request.info.port, reg_request.topiclist])
                status = True
                reason = "The broker name is unique and there is only one broker."
        else:
            raise Exception("Role unknown: Should be either publisher, subscriber, or broker.")
        if len(self.pub_list) >= self.no_pubs and len(self.sub_list) >= self.no_subs:
            self.is_ready = True
        self.mw_obj.handle_register(status, reason)
        return 0
      
    @handle_exception  
    def isready_request(self):
        self.logger.info("DiscoveryAppln:: isready_request")
        self.mw_obj.update_is_ready_status(self.is_ready) # Debugging done: True -> self.is_ready
        return 0
    
    @handle_exception
    def handle_topic_request(self, topic_req):
        self.logger.info("DiscoveryAppln::handle_topic_request - start")
        pubTopicList = []
        for pub in self.pub_list:
            if any(topic in pub[3] for topic in topic_req.topiclist):
                self.logger.info("DiscoveryAppln::handle_topic_request - add pub")
                pubTopicList.append([pub[0], pub[1], pub[2]])     
        self.mw_obj.send_pubinfo_for_topic(pubTopicList)
        return 0
    
    @handle_exception
    def handle_all_publist(self):
        self.logger.info ("DiscoveryAppln:: handle_all_publist")
        pubWithoutTopicList = []
        if len(self.pub_list) != 0:
            for pub in self.pub_list:
                pubWithoutTopicList.append([pub[0], pub[1], pub[2]])
        else:
            pubWithoutTopicList = []
        self.mw_obj.send_all_pub_list(pubWithoutTopicList)
        return 0

    """
    New methods for PA2
    1. get_node_type_and_index: This function retrieves the type and index of a node in the distributed hash table based 
    on a given key.
    2. forward_request: This function sends a forward request to a node in the distributed hash table based on the type 
    and index of the node.
    1. registerForwardDHT: performs Chord algorithm to find the successor node and forwards the registration request accordingly.
    2. isreadyForwardDHT: aggregates the number of publishers and subscribers, checks if there are any brokers, 
    and forwards the is-ready request to the appropriate node.
    3. lookupPubByTopicForwardDHT: finds the responsible node for the requested topic and returns the information of 
    all publishers subscribed to that topic, or forwards the request to the appropriate node.
    4. lookallForwardDHT: forwards the look-all request to the appropriate node in the Chord DHT.
    5. process_topic: This function processes a new topic to be registered in the distributed hash table by 
    calculating its hash value and sending a registration request to the appropriate node.
    6. registerRequestDHT: encodes the registration request and sends it to the appropriate node in the Chord DHT.
    7. isreadyRequestDHT: encodes the is-ready request and sends it to the appropriate node in the Chord DHT.
    8. lookupRequestDHT: Encodes a lookup request for a topic into a message to be sent to the appropriate node in 
    the Chord DHT.
    9. lookallRequestDHT: Encodes a request to lookup all publishers into a message to be sent to the appropriate node 
    in the Chord DHT.
    """
    @handle_exception
    def get_node_type_and_index(self, key):
        index = self.getSuccessor(self.hash, key)
        if index == self.hash:
            nodeType = discovery_pb2.NODE_SUCCESSOR
        else:
            nodeType = discovery_pb2.NODE_FORWARD
        return nodeType, index

    @handle_exception
    def forward_request(self, forward_func, *args, **kwargs):
        key = kwargs.get('key')
        nodeType, index = self.get_node_type_and_index(key)
        forward_func(nodeType, index, *args)

    @handle_exception
    def registerForwardDHT(self, register_req, key):
        self.logger.info("DiscoveryAppln::registerForwardDHT - start")
        self.forward_request(self.mw_obj.forward_register_req, register_req, key=key)

    @handle_exception
    def isreadyForwardDHT(self, isready_req, key):
        self.logger.info("DiscoveryAppln::isreadyForwardDHT - start")
        no_pubs = isready_req.no_pubs + self.no_current_pubs
        no_subs = isready_req.no_subs + self.no_current_subs
        broker = isready_req.broker or len(self.broker_list) > 0
        self.forward_request(self.mw_obj.forward_isready_req, no_pubs, no_subs, broker, key=key)

    @handle_exception
    def lookupPubByTopicForwardDHT(self, lookup_req, key):
        self.logger.info("DiscoveryAppln::lookupPubByTopicForwardDHT - start")
        nodeType, index = self.get_node_type_and_index(key)
        if nodeType == discovery_pb2.NODE_SUCCESSOR:
            pubTopicList = [[pub[0], pub[1], pub[2]] for pub in self.pub_list if lookup_req.topic in pub[3]]
            self.mw_obj.send_pubinfo_for_topic(pubTopicList)
        else:
            self.forward_request(self.mw_obj.forward_lookup_pub_by_topic_req, lookup_req.topic, key=key)

    @handle_exception
    def lookallForwardDHT(self, lookall_req, key):
        self.logger.info("DiscoveryAppln::lookallForwardDHT - start")
        self.forward_request(self.mw_obj.forward_lookall_req, lookall_req, key=key)

    @handle_exception
    def process_topic(self, topic, reg_req):
        self.logger.info("DiscoveryAppln::process_topic - start")
        hashValue = self.hashFunction(topic)
        nodeType, index = self.get_node_type_and_index(hashValue)
        self.mw_obj.register_req_dht(index, nodeType, hashValue, reg_req, topic)

    @handle_exception
    def registerRequestDHT(self, reg_req):
        self.logger.info("DiscoveryAppln::registerRequestDHT - start")
        for topic in reg_req.topiclist:
            self.process_topic(topic, reg_req)

    @handle_exception
    def isreadyRequestDHT(self):
        self.logger.info("DiscoveryAppln::isreadyRequestDHT - start")
        broker = (len(self.broker_list) > 0)
        nodeType = discovery_pb2.NODE_FORWARD
        self.mw_obj.isready_req_dht(self.no_current_pubs, self.no_current_subs, broker, nodeType)

    @handle_exception
    def lookupRequestDHT(self, lookup_req):
        self.logger.info("DiscoveryAppln::lookupRequestDHT - start")
        hashValue = self.hashFunction(lookup_req.topic)
        nodeType, index = self.get_node_type_and_index(hashValue)
        self.mw_obj.lookup_pub_by_topic_req_dht(lookup_req.topic, nodeType, hashValue)
        
    @handle_exception
    def lookallRequestDHT(self):
        self.logger.info("DiscoveryAppln::lookallRequestDHT - start")
        nodeType, index = self.get_node_type_and_index(self.hash)
        self.mw_obj.lookup_all_pubs_req_dht(nodeType, index)

    @handle_exception
    def dump(self):
        self.logger.info("**********************************")
        self.logger.info("DiscoveryAppln::dump")
        self.logger.info("     Name: {}".format (self.name))
        self.logger.info("     Number of publishers: {}".format (self.no_pubs))
        self.logger.info("     Number of subscribers: {}".format (self.no_subs))
        self.logger.info("     Number of brokers: {}".format(self.no_broker))
        self.logger.info("     Number of nodes in distributed hash table: {}".format(self.M))
        self.logger.info("     Lookup: {}".format (self.lookup))
        self.logger.info("     Dissemination: {}".format (self.dissemination))
        super().dumpFingerTable()
        self.logger.info ("**********************************")
 
def parseCmdLineArgs ():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser (description="Discovery Application")
    # Now specify all the optional arguments we support
    parser.add_argument("-n", "--name", default="discovery", help="Discovery")
    parser.add_argument("-a", "--addr", default="localhost", help="IP addr of this publisher to advertise (default: localhost)")
    parser.add_argument("-p", "--port", type=int, default=5555, help="Port number on which our underlying publisher ZMQ service runs, default=5577")
    parser.add_argument("-P", "--no_pubs", type=int, default=1, help="Number of publishers")
    parser.add_argument("-S", "--no_subs", type=int, default=1, help="Number of subscribers")
    parser.add_argument("-B", "--no_broker", type=int, default=1, help="Number of brokers")
    parser.add_argument("-T", "--num_topics", type=int, choices=range(1,10), default=1, help="Number of topics to publish, currently restricted to max of 9")
    parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")
    parser.add_argument("-f", "--frequency", type=int,default=1, help="Rate at which topics disseminated: default once a second - use integers")
    parser.add_argument("-i", "--iters", type=int, default=1000, help="number of publication iterations (default: 1000)")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
    # New code for PA2
    parser.add_argument ("-j", "--dht_json", default="dht.json", help="JSON file with all DHT nodes, default dht.json")
    return parser.parse_args()
    
def main():
    try:
        logging.info("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("DiscoveryAppln")
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()
        logger.debug("Main: resetting log level to {}".format (args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is {}".format (logger.getEffectiveLevel ()))
        logger.debug("Main: obtain the Discovery appln object")
        discovery_app = DiscoveryAppln(logger)
        logger.debug("Main: configure the Discovery appln object")
        discovery_app.configure(args)
        logger.debug("Main: invoke the Discovery appln driver")
        discovery_app.driver()
    except Exception as e:
        logger.error("Exception caught in main - {}".format (e))
        return

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()