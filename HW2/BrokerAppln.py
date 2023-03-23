import time   # for sleep
import argparse # for argument parsing
import configparser # for configuration parsing
import logging # for logging. Use it in place of print statements.
from topic_selector import TopicSelector
from CS6381_MW.BrokerMW import BrokerMW
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2
from ChordDHT import Pingu
from functools import wraps

from enum import Enum  # for an enumeration we are using to describe what state we are in

class BrokerAppln(Pingu):
    class State (Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        CHECKMSG = 4,
        RECEIVEANDDISSEMINATE = 5,
        COMPLETED = 6
    
    def handle_exception(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                raise e
        return wrapper
    
    def __init__ (self, logger):
        super().__init__(logger)
        self.state = self.State.INITIALIZE # state that are we in
        self.name = None # our name (some unique name)
        self.topiclist = None # the different topics that we publish on
        self.iters = None   # number of iterations of publication
        self.frequency = None # rate at which dissemination takes place
        self.mw_obj = None # handle to the underlying Middleware object
        self.msg_list = [] # Initalise to empty list
        self.lookup = None # one of the diff ways we do lookup
        self.dissemination = None # direct or via broker
        self.is_ready = None
    
    @handle_exception
    def configure(self, args):
        self.logger.info("BrokerAppln::configure")
        self.state = self.State.CONFIGURE
        self.name = args.name # our name
        self.iters = args.iters  # num of iterations
        self.frequency = args.frequency # frequency with which topics are disseminated
        config = configparser.ConfigParser()
        config.read(args.config)
        self.lookup = config["Discovery"]["Strategy"]
        self.dissemination = config["Dissemination"]["Strategy"]
        ip, port = super().readDhtJson(args)
        self.mw_obj = BrokerMW(self.logger)
        self.mw_obj.configure(args, ip, port) # pass remainder of the args to the m/w object
        self.topiclist = ["weather", "humidity", "airquality", "light", "pressure", "temperature", "sound", "altitude", "location"] # Subscribe to all topics
        self.logger.info("BrokerAppln::configure - configuration complete")
    
    @handle_exception
    def driver(self):
        self.logger.info("BrokerAppln::driver")
        self.dump()
        self.logger.info("BrokerAppln::driver - upcall handle")
        self.mw_obj.set_upcall_handle(self)
        self.state = self.State.REGISTER
        self.mw_obj.event_loop(timeout=0)  # start the event loop
        self.logger.info("BrokerAppln::driver completed")

    @handle_exception    
    def invoke_operation(self):
        self.logger.info("BrokerAppln::invoke_operation")
        if self.state == self.State.REGISTER:
            self.logger.info("BrokerAppln::invoke_operation - register with the discovery service")
            self.mw_obj.register(self.name, self.topiclist)
            return None
        elif self.state == self.State.ISREADY:
            self.logger.info("BrokerAppln::invoke_operation - check if are ready to go")
            self.mw_obj.is_ready()  # send the is_ready? request
            return None
        elif self.state == self.State.CHECKMSG:
            self.logger.info("BrokerAppln::invoke_operation - start checking messages from all publishers")
            self.mw_obj.receiveAllPublishers()
            return None
        elif self.state == self.State.RECEIVEANDDISSEMINATE:
            while True:
                self.logger.info("BrokerAppln::invoke_operation - RECEIVING AND SENDING SIMULATANEOUSLY:")
                msg = self.mw_obj.receive_msg_sub()
                self.mw_obj.send_msg_pub(msg + ":(from broker)")
                self.msg_list.append(msg)
                self.logger.info("BrokerAppln::invoke_operation - msg: " + str(msg))
                return None
        elif self.state == self.State.COMPLETED:
            self.mw_obj.disable_event_loop()
            self.logger.info ("BrokerAppln::invoke_operation - Completed")
            return None
        else:
            raise ValueError ("Undefined state of the appln object")
        
    @handle_exception
    def register_response(self, reg_resp):
        self.logger.info ("BrokerAppln::register_response")
        if (reg_resp.status == discovery_pb2.STATUS_SUCCESS):
            self.logger.info("BrokerAppln::register_response - registration is a success")
            self.state = self.State.ISREADY  
            return 0  
        else:
            self.logger.info("BrokerAppln::register_response - registration is a failure with reason {}".format (reg_resp.reason))
            raise ValueError ("Broker needs to have unique id")
        
    # handle isready response method called as part of upcall
    # Also a part of upcall handled by application logic
    @handle_exception
    def isready_response(self, isready_resp):
        self.logger.info("BrokerAppln::isready_response")
        if not isready_resp.status:
            self.logger.info("BrokerAppln::driver - Not ready yet; check again")
            time.sleep(10)  # sleep between calls so that we don't make excessive calls
        else:
            # we got the go ahead + set the state to CHECKMSG
            self.state = self.State.CHECKMSG
        return 0

    @handle_exception
    def allPublishersResponse(self, check_response):
        self.logger.info("BrokerAppln::allPublishersResponse")
        for pub in check_response.publist:
            self.logger.info("tcp://{}:{}".format(pub.addr, pub.port))
            self.mw_obj.connect2pubs(pub.addr, pub.port)
        self.state = self.State.RECEIVEANDDISSEMINATE #RECEIVEFROMPUB
        return 0
    
    @handle_exception
    def dump(self):
        self.logger.info("**********************************")
        self.logger.info("BrokerAppln::dump")
        self.logger.info("     Name: {}".format (self.name))
        self.logger.info("     Number of nodes in distributed hash table: {}".format(self.M))
        self.logger.info("     Lookup: {}".format (self.lookup))
        self.logger.info("     Dissemination: {}".format (self.dissemination))
        self.logger.info("     Iterations: {}".format (self.iters))
        self.logger.info("     Frequency: {}".format (self.frequency))
        super().dumpFingerTable()
        self.logger.info ("**********************************")

# Parse command line arguments
def parseCmdLineArgs ():
    # instantiate a ArgumentParser object
    parser = argparse.ArgumentParser(description="Broker Application")
    # Now specify all the optional arguments we support
    parser.add_argument("-n", "--name", default="broker", help="broker")
    parser.add_argument("-a", "--addr", default="localhost", help="IP addr of this broker to advertise (default: localhost)")
    parser.add_argument("-p", "--port", type=int, default=5578, help="Port number on which our underlying Broker ZMQ service runs, default=5576")
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default dht.json") # changed to dht.json from localhost:5555
    parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")
    parser.add_argument("-f", "--frequency", type=int,default=1, help="Rate at which topics disseminated: default once a second - use integers")
    parser.add_argument("-i", "--iters", type=int, default=1000, help="number of publication iterations (default: 1000)")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, choices=[logging.DEBUG,logging.INFO,logging.WARNING,logging.ERROR,logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
    # New code for PA2
    parser.add_argument ("-j", "--dht_json", default="dht.json", help="JSON file with all DHT nodes, default dht.json")
    return parser.parse_args()

def main():
    try:
        # obtain a system wide logger and initialize it to debug level to begin with
        logging.info("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("BrokerAppln")
        # first parse the arguments
        logger.debug ("Main: parse command line arguments")
        args = parseCmdLineArgs()
        # reset the log level to as specified
        logger.debug("Main: resetting log level to {}".format (args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is {}".format (logger.getEffectiveLevel ()))
        # Obtain a Broker application
        logger.debug("Main: obtain the Broker appln object")
        broker_app = BrokerAppln (logger)
        # configure the object
        logger.debug("Main: configure the Broker appln object")
        broker_app.configure(args)
        # now invoke the driver program
        logger.debug("Main: invoke the Broker appln driver")
        broker_app.driver()

    except Exception as e:
        logger.error ("Exception caught in main - {}".format (e))
        return

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()