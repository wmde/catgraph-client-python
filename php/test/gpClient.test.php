<?php
require_once('gpTestBase.php');

$gpTestGraphName = 'test' . getmypid();
$gpTestFilePrefix = '/tmp/gptest-' . getmypid();

/**
 * Tests the TCP client connection
 */
class gpClientTest extends gpClientTestBase
{

	//// Client Connection Tests /////////////////////////////////////////////////
	// currently none. Could test handling of TCP issues, etc


}

//TODO: (optionally) start server instance here! let it die when the test script dies.

//TODO: CLI interface behaviour of server (port config, etc)
