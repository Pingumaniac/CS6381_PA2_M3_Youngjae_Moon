import time   # for sleep
import argparse  # for argument parsing
import configparser  # for configuration parsing
import logging  # for logging. Use it in place of print statements.
from topic_selector import TopicSelector
from CS6381_MW.PublisherMW import PublisherMW
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2
from enum import Enum  # for an enumeration we are using to describe what state we are in
from datetime import datetime

class PublisherAppln():
    class State (Enum):
        INITIALIZE = 0,
        CONFIGURE = 1,
        REGISTER = 2,
        ISREADY = 3,
        DISSEMINATE = 4,
        COMPLETED = 5

    def __init__(self, logger):
        self.name = None  # our name (some unique name)
        self.topiclist = None  # the different topics that we publish on
        self.iters = None   # number of iterations of publication
        self.frequency = None  # rate at which dissemination takes place
        self.num_topics = None  # total num of topics we publish
        self.mw_obj = None  # handle to the underlying Middleware object
        self.logger = logger  # internal logger for print statements
        self.state = self.State.INITIALIZE  # state that are we in
        self.lookup = None  # one of the diff ways we do lookup
        self.dissemination = None  # direct or via broker

    def configure(self, args):
        try:
            self.logger.info("PublisherAppln::configure")
            self.state = self.State.CONFIGURE
            self.name = args.name  # our name
            self.iters = args.iters  # num of iterations
            self.frequency = args.frequency  # frequency with which topics are disseminated
            self.num_topics = args.num_topics  # total num of topics we publish
            config = configparser.ConfigParser()
            config.read(args.config)
            self.lookup = config["Discovery"]["Strategy"]
            self.dissemination = config["Dissemination"]["Strategy"]
            self.logger.info(
                "PublisherAppln::configure - selecting our topic list")
            self.selectTopics()
            self.logger.info(
                "PublisherAppln::configure - initialize the middleware object")
            self.mw_obj = PublisherMW(self.logger)
            # pass remainder of the args to the m/w object
            self.mw_obj.configure(args)
            self.logger.info(
                "PublisherAppln::configure - configuration complete")
        except Exception as e:
            raise e

    def driver(self):
        try:
            self.logger.info("PublisherAppln::driver")
            # dump our contents (debugging purposes)
            self.dump()
            self.logger.info("PublisherAppln::driver - upcall handle")
            self.mw_obj.set_upcall_handle(self)
            self.state = self.State.REGISTER
            self.mw_obj.event_loop(timeout=0)  # start the event loop
            self.logger.info("PublisherAppln::driver completed")
        except Exception as e:
            raise e

    def invoke_operation(self):
        try:
            self.logger.info("PublisherAppln::invoke_operation")
            if (self.state == self.State.REGISTER):
                self.logger.info(
                    "PublisherAppln::invoke_operation - register with the discovery service")
                self.mw_obj.register(self.name, self.topiclist)
                return None
            elif (self.state == self.State.ISREADY):
                self.logger.info(
                    "PublisherAppln::invoke_operation - check if are ready to go")
                self.mw_obj.is_ready()  # send the is_ready? request
                return None
            elif (self.state == self.State.DISSEMINATE):
                self.logger.info(
                    "PublisherAppln::invoke_operation - start Disseminating")
                # Now disseminate topics at the rate at which we have configured ourselves.
                ts = TopicSelector()
                for i in range(self.iters):
                    for topic in self.topiclist:
                        dissemination_data = ts.gen_publication(topic)
                        current_time = datetime.now().strftime(
                            '%H-%M-%S-%f')[:-3]
                        current_time = str(current_time)
                        # Current time is sent as well
                        self.mw_obj.disseminate(
                            self.name, topic, dissemination_data, current_time)
                    # ensure we get a floating point num
                    time.sleep(1/float(self.frequency))
                self.logger.info(
                    "PublisherAppln::invoke_operation - Dissemination completed")
                self.state = self.State.COMPLETED
                return 0
            elif (self.state == self.State.COMPLETED):
                self.mw_obj.disable_event_loop()
                return None
            else:
                raise ValueError("Undefined state of the appln object")
            self.logger.info("PublisherAppln::invoke_operation completed")
        except Exception as e:
            raise e

    def register_response(self, reg_resp):
        try:
            self.logger.info("PublisherAppln::register_response")
            if (reg_resp.status == discovery_pb2.STATUS_SUCCESS):
                self.logger.info(
                    "PublisherAppln::register_response - registration is a success")
                self.state = self.State.ISREADY
                return 0
            else:
                self.logger.info(
                    "PublisherAppln::register_response - registration is a failure with reason {}".format(reg_resp.reason))
                raise ValueError("Publisher needs to have unique id")
        except Exception as e:
            raise e

    # handle isready response method called as part of upcall
    # Also a part of upcall handled by application logic
    def isready_response(self, isready_resp):
        try:
            self.logger.info("PublisherAppln::isready_response")
            if not isready_resp.status:  # discovery service is not ready yet
                self.logger.debug(
                    "PublisherAppln::driver - Not ready yet; check again")
                # sleep between calls so that we don't make excessive calls
                time.sleep(10)
            else:
                # we got the go ahead + set the state to disseminate
                self.state = self.State.DISSEMINATE
            return 0
        except Exception as e:
            raise e

    def dump(self):
        try:
            self.logger.info("**********************************")
            self.logger.info("PublisherAppln::dump")
            self.logger.info("     Name: {}".format(self.name))
            self.logger.info("     Lookup: {}".format(self.lookup))
            self.logger.info(
                "     Dissemination: {}".format(self.dissemination))
            self.logger.info("     Num Topics: {}".format(self.num_topics))
            self.logger.info("     TopicList: {}".format(self.topiclist))
            self.logger.info("     Iterations: {}".format(self.iters))
            self.logger.info("     Frequency: {}".format(self.frequency))
            self.logger.info("**********************************")
        except Exception as e:
            raise e

    def selectTopics(self):
        topicSelector = TopicSelector()
        self.topiclist = topicSelector.interest(self.num_topics)


def parseCmdLineArgs():
    parser = argparse.ArgumentParser(description="Publisher Application")
    parser.add_argument("-n", "--name", default="pub", help="Some name assigned to us. Keep it unique per publisher")
    parser.add_argument("-a", "--addr", default="localhost", help="IP addr of this publisher to advertise (default: localhost)")
    parser.add_argument("-p", "--port", type=int, default=5570, help="Port number on which our underlying publisher ZMQ service runs, default=5577")
    parser.add_argument("-d", "--discovery", default="localhost:5555", help="IP Addr:Port combo for the discovery service, default dht.json")  # changed from localhost:5555
    parser.add_argument("-T", "--num_topics", type=int, choices=range(1, 10), default=7, help="Number of topics to publish, currently restricted to max of 9")
    parser.add_argument("-c", "--config", default="config.ini", help="configuration file (default: config.ini)")
    parser.add_argument("-f", "--frequency", type=float, default=1, help="Rate at which topics disseminated: default once a second - use integers")
    parser.add_argument("-i", "--iters", type=int, default=1000, help="number of publication iterations (default: 1000)")
    parser.add_argument("-l", "--loglevel", type=int, default=logging.INFO, 
                        choices=[logging.DEBUG, logging.INFO, logging.WARNING, logging.ERROR, logging.CRITICAL], help="logging level, choices 10,20,30,40,50: default 20=logging.INFO")
    # New code for PA2
    parser.add_argument("-j", "--dht_json", default="dht.json", help="JSON file with all DHT nodes, default dht.json")
    return parser.parse_args()


def main():
    try:
        logging.info("Main - acquire a child logger and then log messages in the child")
        logger = logging.getLogger("PublisherAppln")
        logger.debug("Main: parse command line arguments")
        args = parseCmdLineArgs()
        logger.debug("Main: resetting log level to {}".format(args.loglevel))
        logger.setLevel(args.loglevel)
        logger.debug("Main: effective log level is {}".format(logger.getEffectiveLevel()))
        logger.debug("Main: obtain the publisher appln object")
        pub_app = PublisherAppln(logger)
        logger.debug("Main: configure the publisher appln object")
        pub_app.configure(args)
        logger.debug("Main: invoke the publisher appln driver")
        pub_app.driver()
    except Exception as e:
        logger.error("Exception caught in main - {}".format(e))
        return

if __name__ == "__main__":
    logging.basicConfig(level=logging.DEBUG, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    main()
