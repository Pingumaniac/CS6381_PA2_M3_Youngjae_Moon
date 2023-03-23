import zmq  # ZMQ sockets
from CS6381_MW import discovery_pb2
from CS6381_MW import topic_pb2
from CS6381_MW.Common import PinguMW
from functools import wraps

class DiscoveryMW(PinguMW):
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
        self.rep = None # will be a ZMQ ROUTER socket for reply
        self.dealer = {} # will be a ZMQ DEALER socket for request
        self.hash2addr = None # hash -> addr table
        
    # configure/initialize (changed the parameters for PA2)
    @handle_exception
    def configure (self, ip, port, fingerTable, hashDict):
        self.logger.info("DiscoveryMW::configure")
        # self.port = args.port
        # self.addr = args.addr
        self.addr = ip # new code for PA2
        self.port = port  # new code for PA2
        context = zmq.Context()  # returns a singleton object
        self.poller = zmq.Poller()
        self.rep = context.socket(zmq.ROUTER)
        self.poller.register(self.rep, zmq.POLLIN)
        bind_string = "tcp://*:" + str(self.port)
        self.rep.bind(bind_string)
        # self.connectFingerTable(fingerTable, hashDict)
        self.logger.info("DiscoveryMW::configure completed")
        
    # run the event loop where we expect to receive sth
    def event_loop(self, timeout=None):
        super().event_loop("DiscoveryMW", self.rep, timeout)
        
    # Modified for PA2
    @handle_exception
    def handle_request(self):
        self.logger.info("DiscoveryMW::handle_request - start")
        bytesRcvd = self.rep.recv()
        disc_req = discovery_pb2.DiscoveryReq()
        disc_req.ParseFromString(bytesRcvd)
        self.logger.info("DiscoveryMW::handle_request - bytes received")
        timeout = None
        node_type = disc_req.node_type
        msg_type = disc_req.msg_type
        if node_type == discovery_pb2.NODE_INITIAL:
            if msg_type == discovery_pb2.TYPE_REGISTER:
                timeout = self.upcall_obj.registerRequestDHT(disc_req.register_req)
            elif msg_type == discovery_pb2.TYPE_ISREADY:
                timeout = self.upcall_obj.isreadyRequestDHT()
            elif msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                timeout = self.upcall_obj.lookupRequestDHT(disc_req.lookup_req)
            elif msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS:
                timeout = self.upcall_obj.lookallRequestDHT()
        elif node_type == discovery_pb2.NODE_SUCCESSOR:
            if msg_type == discovery_pb2.TYPE_REGISTER:
                timeout = self.upcall_obj.register_request(disc_req.register_req)
            elif msg_type == discovery_pb2.TYPE_ISREADY:
                timeout = self.upcall_obj.isready_request(disc_req.isready_req)
            elif msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                timeout = self.upcall_obj.lookup_request(disc_req.lookup_req)
            elif msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS:
                timeout = self.upcall_obj.lookall_request(disc_req.lookall_req)
        elif node_type == discovery_pb2.NODE_FORWARD:
            if msg_type == discovery_pb2.TYPE_REGISTER:
                timeout = self.upcall_obj.registerForwardDHT(disc_req.register_req, disc_req.key)
            elif msg_type == discovery_pb2.TYPE_ISREADY:
                timeout = self.upcall_obj.isreadyForwardDHT(disc_req.isready_req, disc_req.key)
            elif msg_type == discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC:
                timeout = self.upcall_obj.lookupPubByTopicForwardDHT(disc_req.lookall_req, disc_req.key)
            elif msg_type == discovery_pb2.TYPE_LOOKUP_ALL_PUBS:
                timeout = self.upcall_obj.lookallForwardDHT(disc_req.lookall_req, disc_req.key)
        else: 
            raise ValueError("Unrecognized response message")
        return timeout
    
    @handle_exception
    def handle_register(self, status, reason):
        self.logger.info("DiscoveryMW::handle_register:: check whether the registration has been successful")
        register_response = discovery_pb2.RegisterResp() 
        if status: # if status is true, registration = successful
            register_response.status = discovery_pb2.Status.STATUS_SUCCESS
        else: # otherwise failure
            register_response.status = discovery_pb2.Status.STATUS_FAILURE
        register_response.reason = reason
        discovery_response = discovery_pb2.DiscoveryResp()
        discovery_response.msg_type = discovery_pb2.TYPE_REGISTER
        discovery_response.register_resp.CopyFrom(register_response)
        buf2send = discovery_response.SerializeToString()
        self.rep.send(buf2send)
        self.logger.info("DiscoveryMW::handle_register:: registration status has been checked. plz check the message")
        return 0

    @handle_exception        
    def update_is_ready_status(self, is_ready):
        self.logger.info("DiscoveryMW::update_is_ready_status:: Start this method")
        ready_response = discovery_pb2.IsReadyResp() 
        ready_response.status = is_ready
        discovery_response = discovery_pb2.DiscoveryResp()
        discovery_response.msg_type = discovery_pb2.TYPE_ISREADY
        discovery_response.isready_resp.CopyFrom(ready_response)
        buf2send = discovery_response.SerializeToString()
        self.rep.send(buf2send)
        self.logger.info("DiscoveryMW::update_is_ready_status:: is_ready status sent.")

    @handle_exception
    def send_pubinfo_for_topic(self, pub_in_topic):
        self.logger.info("DiscoveryMW::send_pubinfo_for_topic:: Start this method")
        lookup_response = discovery_pb2.LookupPubByTopicResp() 
        for pub in pub_in_topic:
            reg_info = lookup_response.publisher_info.add()
            reg_info.id = pub[0] # name
            reg_info.addr = pub[1] # addr
            reg_info.port = pub[2] # port
            self.logger.info("DiscoveryMW::send_pubinfo_fo_topic:: Publisher address is tcp://{}:{}".format(reg_info.addr, reg_info.port))
        discovery_response = discovery_pb2.DiscoveryResp()
        discovery_response.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
        discovery_response.lookup_resp.CopyFrom(lookup_response)
        buf2send = discovery_response.SerializeToString()
        self.rep.send(buf2send)  
        self.logger.info ("DiscoveryMW::send_pubinfo_for_topic:: List of publishers sent")
    
    @handle_exception
    def send_all_pub_list(self, pub_list):
        self.logger.info ("DiscoveryMW::send_all_pub_list:: Start this method")
        lookup_response = discovery_pb2.LookupAllPubsResp()
        for pub in pub_list:
            reg_info = lookup_response.publist.add()
            reg_info.id = pub[0] # name
            reg_info.addr = pub[1] # addr
            reg_info.port = pub[2] # port
            self.logger.info("DiscoveryMW::send_all_pub_list:: Publisher address is tcp://{}:{}".format(reg_info.addr, reg_info.port))
        discovery_response = discovery_pb2.DiscoveryResp()
        discovery_response.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
        discovery_response.allpubs_resp.CopyFrom(lookup_response)
        buf2send = discovery_response.SerializeToString()
        self.rep.send(buf2send)
    
    # here we save a pointer (handle) to the application object
    def set_upcall_handle(self, upcall_obj):
        super().set_upcall_handle(upcall_obj)
        
    def disable_event_loop(self):
        super().disable_event_loop()
    
    """
    New functions for PA2
    1. connectFingerTable(finger_table): Connects the DEALER sockets to the nodes in the finger table 
    dictionary by creating and registering ZeroMQ connections with socket options.
    2. send_discovery_req: Sends a discovery request
    3. create_discovery_req: Creates a discovery request
    4. register_req_dht(index, node_type, key, reg_req, topic): Sends a serialized 
    DiscoveryReq message of type TYPE_REGISTER with registration request parameters to 
    a node identified by a key.
    5. isready_req_dht(no_pubs, no_subs, broker, node_type): Sends a serialized DiscoveryReq 
    message of type TYPE_ISREADY with broker parameters to a node identified by the 
    current node's key.
    6. lookup_pub_by_topic_req_dht(topic, node_type, key): Sends a serialized DiscoveryReq 
    message of type TYPE_LOOKUP_PUB_BY_TOPIC with topic parameters to a node identified by a key.
    7. lookup_all_pubs_req_dht(node_type, key): Sends a serialized DiscoveryReq message of 
    type TYPE_LOOKUP_ALL_PUBS to a node identified by a key.
    8. forward_register_req(status, index, node_type, key, reg_req): Sends a multipart message with a 
    serialized DiscoveryReq message of type TYPE_REGISTER with registration request parameters to a 
    node's successor.
    9. forward_isready_req(no_pubs, no_subs, broker, node_type, key): Sends a multipart message with 
    a serialized DiscoveryReq message of type TYPE_ISREADY with broker parameters to a node's 
    successor.
    10. forward_lookup_pub_by_topic_req(topic, node_type, key): Sends a multipart message with a 
    serialized DiscoveryReq message of type TYPE_LOOKUP_PUB_BY_TOPIC with topic parameters to a 
    node's successor.
    11. forward_lookall_req(node_type, index, key, lookall_req): Sends a multipart message with a 
    serialized DiscoveryReq message of type TYPE_LOOKUP_ALL_PUBS with lookup request parameters 
    to a node's successor.
    """
    @handle_exception
    def connectFingerTable(self, finger_table, hash_dict):
        self.logger.debug("DiscoveryMW::connectFingerTable - start")
        context = zmq.Context()
        # Iterates through the finger table
        for hash in finger_table:
            # Converts the hash to a string key
            key = str(hash) 
            # If the key is not already in the dealer dictionary, creates a new DEALER socket
            if key not in self.dealer:
                self.dealer[key] = context.socket(zmq.DEALER)
                # Gets the node corresponding to the key
                node = hash_dict[hash]
                # Constructs the ZeroMQ connection string
                connect_str = "tcp://" + node['IP'] + ":" + str(node['port'])
                # Registers the socket with the ZeroMQ poller
                self.poller.register(self.dealer[key], zmq.POLLIN)
                # This sets the "IDENTITY" option to the given key value, encoded as UTF-8. 
                # The IDENTITY option is used to set a unique identifier for the socket, which is used when communicating 
                # with other ZeroMQ sockets. In this case, the key value is used as the socket identity, which allows 
                # other sockets to address messages to this specific socket.
                self.dealer[key].setsockopt(zmq.IDENTITY, key.encode('utf-8'))
                # Connects to the node
                self.dealer[key].connect(connect_str)
                self.logger.debug(f"DiscoveryMW::connectFingerTable: {key}, {self.dealer[key]} have been connected to {self.hash_to_ip[key]['id']}")
        self.logger.debug("DiscoveryMW::connectFingerTable: connected in dictionary self.dealer")
    
    @handle_exception
    def send_discovery_req(self, index, discovery_req):
        msg = discovery_req.SerializeToString()
        # Sends the serialized DiscoveryReq message to the node using the DEALER socket in the dealer dictionary
        self.dealer[index].send(msg)
        self.logger.debug(f"DiscoveryMW::send_discovery_req - sent to dht: {discovery_req}")

    @handle_exception
    def create_discovery_req(self, node_type, key, reg_req, topic):
        discovery_req = discovery_pb2.DiscoveryReq()
        discovery_req.msg_type = discovery_pb2.TYPE_REGISTER
        discovery_req.node_type = node_type
        discovery_req.key = key
        reg_req.topiclist.append(topic)
        discovery_req.register_req.CopyFrom(reg_req)
        return discovery_req

    @handle_exception
    def register_req_dht(self, index, node_type, key, reg_req, topic):
        self.logger.debug("DiscoveryMW::register_req_dht - start")
        discovery_req = self.create_discovery_req(node_type, key, reg_req, topic)
        self.send_discovery_req(index, discovery_req)
    
    @handle_exception
    def isready_req_dht(self, no_pubs, no_subs, broker, node_type):
        self.logger.debug("DiscoveryMW::isready_req_dht - start")
        discovery_req = discovery_pb2.DiscoveryReq()
        discovery_req.msg_type = discovery_pb2.TYPE_ISREADY
        discovery_req.isready_req.no_pubs = no_pubs
        discovery_req.isready_req.no_subs = no_subs
        discovery_req.isready_req.broker = broker
        discovery_req.node_type = node_type
        # Gets the key for the current node
        keyString = str(self.hash_to_ip[self.hash]['id'])
        self.send_discovery_req(str(keyString), discovery_req)
        self.logger.debug("DiscoveryMW::isready_req_dht - sent to dht: " + str(discovery_req))
    
    @handle_exception
    def lookup_pub_by_topic_req_dht(self, topic, node_type, key):
        self.logger.debug("DiscoveryMW::lookup_pub_by_topic_req_dht - start")
        discovery_req = discovery_pb2.DiscoveryReq()
        discovery_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
        discovery_req.lookup_req.topiclist = topic
        discovery_req.node_type = node_type
        self.send_discovery_req(str(key), discovery_req)
        self.logger.debug("DiscoveryMW::lookup_pub_by_topic_req_dht - sent to dht: " + str(discovery_req))
   
    @handle_exception
    def lookup_all_pubs_req_dht(self, node_type, key):
        self.logger.debug("DiscoveryMW::lookup_all_pubs_req_dht - start")
        discovery_req = discovery_pb2.DiscoveryReq()
        discovery_req.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
        discovery_req.node_type = node_type
        self.send_discovery_req(str(key), discovery_req)
        self.logger.debug("DiscoveryMW::lookup_all_pubs_req_dht - sent to dht: " + str(discovery_req))

    @handle_exception
    def forward_register_req(self, status, index, node_type, key, reg_req):
        self.logger.debug("DiscoveryMW::forward_register_req - start")
        discovery_req = discovery_pb2.DiscoveryReq()
        discovery_req.msg_type = discovery_pb2.TYPE_REGISTER
        discovery_req.index = index
        discovery_req.node_type = node_type
        discovery_req.key = key
        discovery_req.register_req.CopyFrom(reg_req)
        discovery_req.status = status
        buf2send = discovery_req.SerializeToString()
        # Creates a multipart message with the node's key and the serialized DiscoveryReq message
        msg = [key.to_bytes(4, 'big'), buf2send]
        # Sends the byte buffer to the node's successor using the ZeroMQ REP socket
        self.rep.send_multipart(msg)
        self.logger.debug("DiscoveryMW::forward_register_req - response sent")
    
    @handle_exception    
    def forward_isready_req(self, no_pubs, no_subs, broker, node_type, key):
        self.logger.debug("DiscoveryMW::forward_isready_req - start")
        discovery_req = discovery_pb2.DiscoveryReq()
        discovery_req.msg_type = discovery_pb2.TYPE_ISREADY
        discovery_req.isready_req.no_pubs = no_pubs
        discovery_req.isready_req.no_subs = no_subs
        discovery_req.isready_req.broker = broker
        discovery_req.node_type = node_type
        buf2send = discovery_req.SerializeToString()
        # Creates a multipart message with the node's key and the serialized DiscoveryReq message
        msg = [key.to_bytes(4, 'big'), buf2send]
        # Sends the multipart message to the node's successor using the ZeroMQ REP socket
        self.rep.send_multipart(msg)
        self.logger.debug("DiscoveryMW::forward_isready_req - response sent")

    @handle_exception
    def forward_lookup_pub_by_topic_req(self, topic, node_type, key):
        self.logger.debug("DiscoveryMW::forward_lookup_pub_by_topic_req - start")
        discovery_req = discovery_pb2.DiscoveryReq()
        discovery_req.msg_type = discovery_pb2.TYPE_LOOKUP_PUB_BY_TOPIC
        discovery_req.lookup_req.topic = topic
        discovery_req.node_type = node_type
        buf2send = discovery_req.SerializeToString()
        # Creates a multipart message with the node's key and the serialized DiscoveryReq message
        msg = [key.to_bytes(4, 'big'), buf2send]
        # Sends the multipart message to the node's successor using the ZeroMQ REP socket
        self.rep.send_multipart(msg)
        self.logger.debug("DiscoveryMW::forward_lookup_pub_by_topic_req - response sent")

    @handle_exception    
    def forward_lookall_req(self, node_type, index, key, lookall_req):
        self.logger.debug("DiscoveryMW::forward_lookall_req - start")
        discovery_req = discovery_pb2.DiscoveryReq()
        discovery_req.msg_type = discovery_pb2.TYPE_LOOKUP_ALL_PUBS
        discovery_req.index = index
        discovery_req.node_type = node_type
        discovery_req.key = key
        discovery_req.lookall_req.CopyFrom(lookall_req)
        buf2send = discovery_req.SerializeToString()
        # Creates a multipart message with the node's key and the serialized DiscoveryReq message
        msg = [key.to_bytes(4, 'big'), buf2send]
        # Sends the byte buffer to the node's successor using the ZeroMQ REP socket
        self.rep.send_multipart(msg)
        self.logger.debug("DiscoveryMW::forward_lookall_req - response sent")