import TestBase
import os

TestGraphName = 'test' + str(os.getpid())
TestFilePrefix = '/tmp/gptest-' + str(os.getpid())

class ClientTest (TestBase.ClientTestBase):
    """Test the TCP client connection.

    Client Connection Tests
    currently none. Could test handling of TCP issues, etc

    @TODO: (optionally) start server instance here!
          let it die when the test script dies.
          
    @TODO: CLI interface behaviour of server (port config, etc)

    """


