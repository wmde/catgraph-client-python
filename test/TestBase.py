import unittest
import os, sys, traceback
from gp.client import *
from TestConfig import *

TestGraphName = 'test' + str(os.getpid())

def suicide( code = 1 ):
    os._exit(code)

class TestBase:
    """A few static methods to compare lists and sets recursively,
       so the equality of entire data structures can be asserted.
    """
    
    @staticmethod
    def setContains( a, w ):
        """ Checks if a is an element of w. If a is a list, tuple, set or dict,
            a recursive comparison is performed.
        """
        
        found = False
        for v in a:
            if type(v) in (tuple, list):
                try:
                    if TestBase.arrayEquals( v, w ):
                        found = True
                        break
                except (TypeError, AttributeError), e:
                    #perhaps w wasn't iterable
                    pass
            elif type(v) == set:
                try:
                    if TestBase.setEquals( v, w ):
                        found = True
                        break
                except (TypeError, AttributeError), e:
                    #perhaps w wasn't iterable
                    pass
            elif type(v) == dict:
                raise Exception("deep dictionary comparison not yet implemented")
            else:
                if v == w:
                    found = True
        
        return found

    
    @staticmethod
    def setEquals( a, b ):
        """ determins if a and b contain the same elements. a and b my be sets,
            lists or tuples, but do not need to have the same type.
        """
        
        if len(a) != len(b):
            return False
        
        for v in a:
            if not TestBase.setContains(b, v):
                return False
        
        return True
    
    @staticmethod
    def arrayEquals( a, b ):
        if len(a) != len(b):
			return False
        
        k = 0
        for v in a:
            w = b[k]
            
            if type(v) in (tuple, list, set):
                #WARNING: no protection against circular array references
                
                try:
                    if not TestBase.arrayEquals( w, v ):
                        return False
                except:
                    #type error, probably. perhaps w wasn't iterable
                    return False
            elif type(v) == dict:
                raise Exception("deep dictionary comparison not yet implemented")
            elif w != v:
                return False
            
            k += 1
        
        return True


class ConnectionTestBase(TestBase):
    """Abstract base class with basic Connection tests.
    
    These need to pass for all types of connections.
    @Note: lib functionality like try and capture is tested in
           SlaveTest, because it doesn't need to be tested separately
           for all types of connections
           
    """
 
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
        stats = pairs2map(stats)
        self.assertEqual(stats['ArcCount'], 0, "arc count should be zero")
    
    def test_dataSetHandling(self):
        self.gp.add_arcs((( 1, 11 ),( 1, 12 ),( 11, 111 ),( 11, 112 ),))
        self.assertStatus('OK')
        self.assertStatsValue('ArcCount', 4)
        arcs = self.gp.capture_list_successors(1)
        self.assertTrue(ConnectionTestBase.setEquals(
          arcs, [(11,), (12,),]), "sucessors of (1)" )
        arcs = self.gp.capture_list_successors(11)
        self.assertTrue(ConnectionTestBase.setEquals(
          arcs, [(111,), (112,),]), "sucessors of (2)" )         
    
    
    #### utility ######################################################
    def assertNone(self, value, msg = None):
        if value is not None:
            if msg is None:
                msg = "expected None, found %s" % value
                
            self.fail(msg)
        
    def assertEmpty(self, value, msg = None):
        if value:
            if msg is None:
                msg = "expected value to be empty, found %s" % value
                
            self.fail(msg)
        
    def assertNotNone(self, value, msg = None):
        if value is None:
            if msg is None:
                msg = "found None where not expected"
                
            self.fail(msg)
            
    def assertContains(self, k, array, msg = None):
        if not k in array:
            if msg is None:
                msg = "Key %s not found in %s" % (k, array)
                
            self.fail(msg)
            
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
            traceback.print_exc();
            suicide(10)


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
            traceback.print_exc();
            suicide(11)
        try:
            self.gp.authorize(
              'password', TestAdmin + ":" + TestAdminPassword)
        except gpException, ex:
            print("Unable to connect to authorize as " + TestAdmin
              + ", please check the TestAdmin and TestAdminPassword "
              + "configuration options in TestConfig.py.")
            print("Original error: " + str(ex))
            traceback.print_exc();
            suicide(12)
        try:
            self.gp.create_graph(TestGraphName)
        except gpException, ex:
            print("Unable to create graph " + TestGraphName
              + ", please check the TestGraphName configuration option "
              + "in TestConfig.py as well as the privileges of user "
              + TestAdmin + ".")
            print("Original error: " + str(ex))
            traceback.print_exc();
            suicide(13)
        
        self.gp.use_graph(TestGraphName)
        # if use_graph throws an error, let it rip. it really shouldn't
        # happen and it's not a confiugration problem
    
    def newConnection(self):
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

