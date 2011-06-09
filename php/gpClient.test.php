<?php

require_once('gpSlave.test.php');
 
class gpClientTest extends gpSlaveTest
{
	public function setUp() {
		global $gpTestGraphServHost, $gpTestGraphServPort, $gpTestGraphName;
		global $gpTestUser, $gpTestPassword;
		
		$this->gp = new gpClient( null, $gpTestGraphServHost, $gpTestGraphServPort );
		$this->gp->connect();
		
		$this->gp->authorize( 'password', "$gpTestUser:$gpTestPassword" );
		$this->gp->create_graph( $gpTestGraphName );
		$this->gp->use_graph( $gpTestGraphName );
	}
	
	public function tearDown() {
		global $gpTestGraphName;
		
		$this->gp->try_drop_graph( $gpTestGraphName );
	}
}

//TODO: start server instance here! let it die when the test script dies.

#FIXME: hardcoded server...

$gpTestGraphServHost = 'localhost';
$gpTestGraphServPort = GP_PORT;
$gpTestGraphName = 'test' . getmypid();
$gpTestUser = 'fred';
$gpTestPassword = 'test';

