import zmq  # ZMQ sockets
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2
from CS6381_MW.Common import PinguMW

class PublisherMW(PinguMW):
  # constructor
  def __init__(self, logger):
    super().__init__(logger)
    self.req = None # will be a ZMQ REQ socket to talk to Discovery service
    self.pub = None # will be a ZMQ PUB socket for dissemination

  # configure/initialize
  def configure(self, args):
    ''' Initialize the object '''
    try:
      self.logger.info("PublisherMW::configure")
      self.port = args.port
      self.addr = args.addr
      context = zmq.Context()  # returns a singleton object
      self.poller = zmq.Poller()
      self.req = context.socket(zmq.REQ)
      self.pub = context.socket(zmq.PUB)
      self.poller.register(self.req, zmq.POLLIN)
      connect_str = "tcp://" + args.discovery
      self.req.connect(connect_str)
      bind_string = "tcp://*:" + str(self.port)
      self.pub.bind (bind_string)
      self.logger.info("PublisherMW::configure completed")
    except Exception as e:
      raise e

  # run the event loop where we expect to receive sth
  def event_loop(self, timeout=None):
   super().event_loop("PublisherMW", self.req, timeout)
            
  # handle an incoming reply
  def handle_reply(self):
    try:
      self.logger.info("PublisherMW::handle_reply")
      bytesRcvd = self.req.recv()
      discovery_response = discovery_pb2.DiscoveryResp()
      discovery_response.ParseFromString(bytesRcvd)
      if (discovery_response.msg_type == discovery_pb2.TYPE_REGISTER):
        timeout = self.upcall_obj.register_response(discovery_response.register_resp)
      elif (discovery_response.msg_type == discovery_pb2.TYPE_ISREADY):
        timeout = self.upcall_obj.isready_response(discovery_response.isready_resp)
      else:
        raise ValueError("Unrecognized response message")
      return timeout
    except Exception as e:
      raise e
            
  def register(self, name, topiclist):
    super().register("PublisherMW", name, topiclist)

  def is_ready(self):
    super().is_ready("PublisherMW")
    
  def disseminate (self, id, topic, data, current_time):
    try:
      # Now use the protobuf logic to encode the info and send it.  But for now
      # we are simply sending the string to make sure dissemination is working.
      send_str = topic + ":" + id + ":" + data + ":" + current_time
      self.logger.info("PublisherMW::disseminate - {}".format (send_str))
      # send the info as bytes. See how we are providing an encoding of utf-8
      self.pub.send(bytes(send_str, "utf-8"))
    except Exception as e:
      raise e
            
  # here we save a pointer (handle) to the application object
  def set_upcall_handle(self, upcall_obj):
    super().set_upcall_handle(upcall_obj)
        
  def disable_event_loop(self):
    super().disable_event_loop()