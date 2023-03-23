# CS6381_PA2_M3_Youngjae_Moon
CS6381 PA2 Milestone 3 Youngjae_Moon

## Status Update

### What I have done
1. Milestone 1 is all done. 
Here setting up the Chord DHT nodes -> Done
Finger table created at each DHT node using the Chord's approach -> done

2. Testing on Mininet to obtain results

3. Introducing a handle_exception wrapper to make my code cleaner for DiscoveryAppln, DiscoveryMW, 

4. Modified discovery.proto for PA2

5. Modified my code for BrokerMW. It now uses XPUB and XSUB instead of PUB and SUB sockets.

6. Adding descriptions for the new functions introduced for PA2

### What I have almost done.
1. I think I have almost completed Milestone 2 and 3. 

Please have a look at my code. I have made various functions for DiscoveryAppln and DiscoveryMW.

Chord's algorithm to store/query information -> I think I have almost done. Had troubles on debugging small errors.

### What I have not done
1. Experimental results and visualising data

## Descriptions of new functions for PA2

### For DiscoveryAppln,

1. get_node_type_and_index: This function retrieves the type and index of a node in the distributed hash table based 
    on a given key.
    
2. forward_request: This function sends a forward request to a node in the distributed hash table based on the type 
    and index of the node.
   
3. registerForwardDHT: performs Chord algorithm to find the successor node and forwards the registration request accordingly.
    
4. isreadyForwardDHT: aggregates the number of publishers and subscribers, checks if there are any brokers, 
    and forwards the is-ready request to the appropriate node.
    
5. lookupPubByTopicForwardDHT: finds the responsible node for the requested topic and returns the information of 
    all publishers subscribed to that topic, or forwards the request to the appropriate node.

6. lookallForwardDHT: forwards the look-all request to the appropriate node in the Chord DHT.

7. process_topic: This function processes a new topic to be registered in the distributed hash table by 
    calculating its hash value and sending a registration request to the appropriate node.

8. registerRequestDHT: encodes the registration request and sends it to the appropriate node in the Chord DHT.

9. isreadyRequestDHT: encodes the is-ready request and sends it to the appropriate node in the Chord DHT.

10. lookupRequestDHT: Encodes a lookup request for a topic into a message to be sent to the appropriate node in 
    the Chord DHT.

11. lookallRequestDHT: Encodes a request to lookup all publishers into a message to be sent to the appropriate node 
    in the Chord DHT.

### For DiscoveryMW,

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
