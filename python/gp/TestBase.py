import unittest
import os
import sys
from Client import *
from TestConfig import *

TestGraphName = 'test' + str(os.getpid())


class TestBase(unittest.TestCase):
    """A few static methods to compare lists and sets.
    
    The class is implemented with more paranoya in the php version,
    but hopefully this version will be sufficient.
    
    """
    @staticmethod
    def setContains(a, w):
        """Check if a contains w (and w isn't empty)."""
        return(len(set(a) & set([w])) and not len(set([w]) - set(a)))
    @staticmethod
    def setEquals(a, b):
        """Check if a and b contain the same elements.
        
        #? @Daniel: The php implementation only checks if
           b contains only arguments that appear in a,
           but not vice versa.
        """
        return set(a) == set(b)         
    @staticmethod
    def arrayEquals(a, b):
        """Check if a and b contain the same sequence of elements."""
        return a == b
     

class ConnectionTestBase(TestBase):
    """Abstract base class with basic Connection tests.
    
    These need to pass for all types of connections.
    @Note: lib functionality like try and capture is tested in
           SlaveTest, because it doesn't need to be tested separately
           for all types of connections
           
    """
 
    def __init__(self, *args):
        self.gp = None
        if args:
            TestBase.__init__(self, args[0])
        else:
            TestBase.__init__(self)
        
    def setUp(self): #abstract
        raise NotImplementedError('subclasses must override setUp() to'
          + ' store a Connection in self.gp')
    
    def tearDown(self):
        if self.gp:
            self.gp.close()
    
    def test_ping(self):
        pong = self.gp.ping()
        #? nix assert?
    
    def test_stats(self):
        stats = self.gp.capture_stats()
        stats = Client.pairs2map(stats)
        self.assertEqual(stats['ArcCount'], 0, "arc count should be zero")
    
    def test_dataSetHandling(self):
        self.gp.add_arcs((( 1, 11 ),( 1, 12 ),( 11, 111 ),( 11, 112 ),))
        self.assertStatus('OK')
        self.assertStatsValue('ArcCount', 4)
        arcs = self.gp.capture_list_successors(1)
        self.assertTrue(ConnectionTestBase.setEquals(
          arcs, ((11), (12),)), "sucessors of (1)" )
        arcs = self.gp.capture_list_successors(11)
        self.assertTrue(ConnectionTestBase.setEquals(
          arcs, ((111), (112),)), "sucessors of (2)" )         
    
    
    #### utility ######################################################
    def assertStatsValue(self, field, value):
        stats = self.gp.capture_stats()
        stats = pairs2map(stats)
        self.assertEquals(value, stats[field], "status[" + field + "]")         
    
    def assertSessionValue(self, field, value):
        stats = self.gp.capture_session_info()
        stats = pairs2map(stats)        
        self.assertEquals(value, stats[field], "session_info[" + field + "]")
    
    def assertStatus(self, value, message=None):
        status = self.gp.getStatus()
        self.assertEquals(value, status, message)


class SlaveTestBase(ConnectionTestBase): #abstract
 
    def setUp(self):
        global TestGraphCorePath
        self.dump = PipeSink(sys.stdout)
        
        try:
            self.gp = Connection.new_slave_connection(TestGraphCorePath)
            self.gp.connect()
        except gpException, ex:
            print("Unable to launch graphcore instance from "
              + "TestGraphCorePath, please make sure graphcore is "
              + "installed and check the TestGraphCorePath "
              + "configuration options in TestConfig.py.")
            print("Original error: " + str(ex))
            quit(10)


class ClientTestBase(ConnectionTestBase): #abstract
 
    def setUp(self):
        global TestAdmin, TestAdminPassword
        global TestGraphName
        global TestGraphServHost, TestGraphServPort
        
        try:
            self.gp = self.newConnection()
        except gpException, ex:
            print("Unable to connect to "
              + "TestGraphServHost:TestGraphServPort, please make sure "
              + "the graphserv process is running and check the "
              + "TestGraphServHost and TestGraphServPort configuration "
              + "options in TestConfig.py.")
            print("Original error: " + str(ex))
            quit(11)
        try:
            self.gp.authorize(
              'password', TestAdmin + ":" + TestAdminPassword)
        except gpException, ex:
            print("Unable to connect to authorize as " + TestAdmin
              + ", please check the TestAdmin and TestAdminPassword "
              + "configuration options in TestConfig.py.")
            print("Original error: " + str(ex))
            quit(12)
        try:
            self.gp.create_graph(TestGraphName)
        except gpException, ex:
            print("Unable to create graph " + TestGraphName
              + ", please check the TestGraphName configuration option "
              + "in TestConfig.py as well as the privileges of user "
              + TestAdmin + ".")
            print("Original error: " + str(ex))
            quit(13)
        
        self.gp.use_graph(TestGraphName)
        # if use_graph throws an error, let it rip. it really shouldn't
        # happen and it's not a confiugration problem
    
    @staticmethod
    def newConnection():
        global TestGraphServHost, TestGraphServPort
        
        gp = Connection.new_client_connection(
          None, TestGraphServHost, TestGraphServPort)
        gp.connect()
        return gp
    
    def tearDown(self):
        global TestGraphName
        global TestAdmin, TestAdminPassword
        
        try:
            self.gp.drop_graph(TestGraphName)
        except gpProtocolException, ex:
            #failed to remove graph, maybe the connection is gone? try again.
            try:
                gp = self.newConnection()
                gp.authorize('password',
                  TestAdmin + ":" + TestAdminPassword)
                gp.drop_graph(TestGraphName)
            except gpException, ex:
                # just give up
                # pass
                raise ex
                
        ConnectionTestBase.tearDown(self)

