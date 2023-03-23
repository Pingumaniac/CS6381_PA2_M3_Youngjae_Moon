import json
import hashlib
from CS6381_MW.DiscoveryMW import DiscoveryMW
from functools import wraps

class Pingu:
    def handle_exception(func):
        @wraps(func)
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except Exception as e:
                raise e
        return wrapper
    
    def __init__(self, logger):
        self.logger = logger  # internal logger for print statements
        self.fingerTable = {} # finger table, initialise to empty list
        self.M = 48  
        self.N = 2**self.M
        self.no_discovery = 20
        self.dht_json = None
        self.id = None
        self.hash = None
        self.hashID = None
        self.hashList = []
        self.nodePointer = None # starts with 1
        self.nodes = {}
        self.dictForDHT = {}
        self.hashDict = {}
        self.mw_obj = DiscoveryMW(self.logger)
    
    @handle_exception
    def readDhtJson(self, args):
        self.logger.info ("Pingu::readDhtJson - start")
        self.dht_json = args.dht_json
        with open (self.dht_json, "r") as f:
            dhtDatabase = json.load(f)
            dhtJson = dhtDatabase['dht']

        # Append data into dht, and hash_list
        ip = None # Initialize ip with a default value
        port = None # Initialize port with a default value
        for nodeInDht in dhtJson:
            self.dictForDHT[nodeInDht['hash']] = nodeInDht
            self.hashList.append(nodeInDht['hash'])
            self.hashDict[nodeInDht['hash']] = {"IP": nodeInDht["IP"], "port": nodeInDht["port"], "id": nodeInDht["id"]}  # Populate the hashToIP dictionary
            nodeID = nodeInDht['id']
            nodeIP = nodeInDht['IP']
            nodePort = nodeInDht['port']
            self.hash = nodeInDht['hash']
            if nodeID == args.name:
                self.id = nodeID
                ip = nodeIP
                port = nodePort
                break
            
        if ip is None: # Check if ip has been assigned a value
            raise ValueError("Could not find matching node in DHT")
                
        self.hashList.sort()
        for i in range(len(self.hashList)):
            if self.hash == self.hashList[i]:
                self.nodePointer = i + 1
                break
                
        self.logger.info("Pingu::readDhtJson - generate finger table")
        self.setFingerTable()
        self.logger.info("Pingu::readDhtJson - end")
        return ip, port

    @handle_exception    
    def setFingerTable(self):
        self.logger.info ("Pingu::setFingerTable - start")
        for i in range(self.M):
            start = (self.hash + 2 ** i) % (2 ** self.M)
            self.fingerTable[start] = self.getNextNode(start)
        self.logger.info ("Pingu::setFingerTable - end")

    @handle_exception
    def getNextNode(self, currentNode):
        if currentNode > self.hashList[-1]:
            return self.hashList[0]
        for i in range(len(self.hashList) - 1):
            if currentNode <= self.hashList[i + 1] and currentNode > self.hashList[i]:
                return self.hashList[i + 1]
        self.logger.info ("Pingu::getNextNode - " + str(self.hashList[0]))
        return self.hashList[0]

    @handle_exception
    def getSuccessor(self, node, key):
        self.logger.info ("Pingu::getSuccessor - start")
        successorNode = self.fingerTable[(self.hash + 1) % (2 ** self.M)]
        if successorNode < key and key <= node:
            preceding_node = self.getClosestPrecedingNode(node, key) # node parameter is inserted
            return self.getSuccessor(preceding_node, key)
        else:
            return successorNode
    
    @handle_exception
    def getClosestPrecedingNode(self, node, key):
        self.logger.info ("Pingu::getClosestPrecedingNode - start")
        for i in range(self.M, 0, -1):
            idx = (self.hash + 2 ** (i - 1)) % (2 ** self.M)
            if self.fingerTable[idx] < key and self.fingerTable[idx] > node:
                return self.fingerTable[idx]
        self.logger.info ("Pingu::getClosestPrecedingNode - " + node)
        return node
    
    @handle_exception
    def dumpFingerTable(self):
        self.logger.info("Finger Table for this entity:")
        for currentNode in self.fingerTable:
            successorNode = self.fingerTable[currentNode]
            self.logger.info("Current node: {}, Successor node:{}".format(currentNode, successorNode))
    
    @handle_exception
    def hashFunction(self, topic):
        self.logger.info("Pingu::hashFunction - start")
        hashString = topic
        hashDigest = hashlib.sha256(bytes (hashString, "utf-8")).digest ()  # this is how we get the digest or hash value
        noOfBytes = int(self.M / 8)  # otherwise we get float which we cannot use below
        hashValue = int.from_bytes (hashDigest[:noOfBytes], "big")  # take lower N number of bytes
        return hashValue