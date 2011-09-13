from gp.client import Connection
from gp.client import gpException
from test_config import *
import test_config
import os
import random
import time

TestGraphName = 'test' + str(os.getpid())

def fuzz_pick( a ):
    i = random.randint(0, len(a)-1)
    return a[i]
     

class FuzzerBase (object): # abstract
    """Test the TCP client connection"""
    
    def __init__(self):
        self.graph = None
        self.useTempGraph = True
    
    def newConnection(self):
        global TestGraphServHost, TestGraphServPort #noetig?
        gp = Connection.new_client_connection(None,
          TestGraphServHost, TestGraphServPort )
        gp.connect()
        return gp
    
    def connect(self):
        global TestAdmin, TestAdminPassword
        global TestGraphName
        global TestGraphServHost, TestGraphServPort
        
        if not self.graph:
            self.graph = TestGraphName
        
        try:
            self.gp = self.newConnection()
        except gpException as ex:
            print("Unable to connect to "
              + TestGraphServHost + ":" + str(TestGraphServPort)
              + ", please make sure the graphserv process is running "
              + "and check the TestGraphServHost and "
              + "TestGraphServPort configuration options in "
              + "test_config.py.")
            print("Original error: " + str(ex))
            quit(11)
        
        try:
            self.gp.authorize( 'password',
              TestAdmin + ":" + TestAdminPassword)
        except gpException, ex:
            print("Unable to connect to authorize as "
              + TestAdmin + ", please check the gpTestAdmin and "
              + "TestAdminPassword configuration options in "
              + "test_config.py.")
            print("Original error: " + str(ex))
            quit(12)
        
        if self.useTempGraph:
            self.gp.try_create_graph( self.graph )
        
        try:
            self.gp.use_graph( self.graph )
        except gpException, ex:
            print("Unable to use graph self.graph, please check the "
              + "TestGraphName configuration option in test_config.py "
              + "as well as the privileges of user " + gpTestAdmin + ".")
            print("Original error: " + ex.getMessage())
            quit(13)
    
    def disconnect(self):
        global TestAdmin, TestAdminPassword

        if self.useTempGraph and self.graph:
            self.gp.try_drop_graph(self.graph) #? gp OK?
    
    def prepare(self):
        pass #noop
    
    def doFuzz(self): #abstract
        raise NotImplementedError(
          "FuzzerBase.doFuzz() not implemented.")
    
    def run(self, argv):
        self.connect()
        self.prepare()
        
        n = None
        if len(argv) > 1:
            n = int(argv[1])
        if not n:
            n = 100

        for k in range(n):
            for i in range(100):
                ok = self.doFuzz()
                if ok:
                    print "+",
                else:
                    print "-",
            
            print "\n";
            # time.sleep(1) #? Muss wieder rein!
        
        self.disconnect()
