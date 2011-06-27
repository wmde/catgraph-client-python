<?php
require_once('gpTestBase.php');

$gpTestGraphName = 'test' . getmypid();
$gpTestFilePrefix = '/tmp/gptest-' . getmypid();

class gpClientTest extends gpConnectionTestBase
{
	public function setUp() {
		global $gpTestAdmin, $gpTestAdminPassword;
		global $gpTestGraphName;
		
		$this->gp = $this->newConnection(); //FIXME: show config/setup error if connection fails!
		
		$this->gp->authorize( 'password', "$gpTestAdmin:$gpTestAdminPassword" );
		$this->assertStatus('OK'); //FIXME: better error message, this is a config/setup error, not an assertion failure!
		
		$this->gp->create_graph( $gpTestGraphName );
		$this->assertStatus('OK'); //FIXME: better error message, this is a config/setup error, not an assertion failure!

		$this->gp->use_graph( $gpTestGraphName );
		$this->assertStatus('OK'); //FIXME: better error message, this is a config/setup error, not an assertion failure!
	}
	
	public function newConnection() {
		global $gpTestGraphServHost, $gpTestGraphServPort;
		
		$gp = new gpClient( null, $gpTestGraphServHost, $gpTestGraphServPort );
		$gp->connect();

		return $gp;
	}
	
	public function tearDown() {
		global $gpTestGraphName;
		
		$this->gp->try_drop_graph( $gpTestGraphName );
		parent::tearDown();
	}

	//// Server Functions ///////////////////////////////////////////////////////
	public function testCreateGraph() {
		global $gpTestGraphName;
		
		$name = $gpTestGraphName."_2";
		
		$this->gp->create_graph($name);
		$ok = $this->gp->try_create_graph($name);
		$this->assertFalse( $ok, "should not be able to create graph again when it already exists" );
		
		$gp = $this->newConnection();
		
		// see if we can use and drop the graph
		$gp->use_graph($name);
		$this->gp->drop_graph($name);

		// see if we can re-create the graph after it was dropped
		$this->gp->create_graph($name); 
		$this->gp->drop_graph($name);
		
		//TODO: test name restrictions
	}

	public function testCreateGraphPrivilege() {
		global $gpTestGraphName;
		global $gpTestAdmin, $gpTestAdminPassword;
		global $gpTestMaster, $gpTestMasterPassword;
		
		$name = $gpTestGraphName."_2";
		
		$gp = $this->newConnection();
		
		$ok = $gp->try_create_graph($name);
		$this->assertFalse( $ok, "should not be able to create a graph without authorizing" );

		$gp->authorize('password', "$gpTestMaster:$gpTestMasterPassword");
		$ok = $gp->try_create_graph($name);
		$this->assertFalse( $ok, "should not be able to create a graph without admin privileges" );

		$gp->authorize('password', "$gpTestAdmin:$gpTestAdminPassword"); // re-authenticate
		$ok = $gp->create_graph($name);
		$this->assertEquals( $ok, 'OK', "should be able to create graph with admin privileges" );
		
		$gp->try_drop_graph($name); // cleanup
	}

	public function testDropGraph() {
		global $gpTestGraphName;
		
		$name = $gpTestGraphName."_2";
		
		$this->gp->create_graph($name);
		$this->gp->drop_graph($name);
		
		$ok = $this->gp->try_use_graph($name);
		$this->assertFalse( $ok, "should not be able to use graph after dropping it" );
		
		#FIXME: pending bug, currently triggers FAILED instead of NONE
		#$ok = $this->gp->try_drop_graph($name);
		#$this->assertEquals( 'NONE', "should not be able to drop graph again after it was already dropped" );
	}

	public function testDropGraphPrivilege() {
		global $gpTestGraphName;
		global $gpTestAdmin, $gpTestAdminPassword;
		global $gpTestMaster, $gpTestMasterPassword;
		
		$name = $gpTestGraphName;
		
		$gp = $this->newConnection();
		
		$ok = $gp->try_drop_graph($name);
		$this->assertFalse( $ok, "should not be able to drop a graph without authorizing" );

		$gp->authorize('password', "$gpTestMaster:$gpTestMasterPassword");
		$ok = $gp->try_drop_graph($name);
		$this->assertFalse( $ok, "should not be able to drop a graph without admin privileges" );

		$gp->authorize('password', "$gpTestAdmin:$gpTestAdminPassword"); // re-authenticate
		$ok = $gp->drop_graph($name);
		$this->assertEquals( $ok, 'OK', "should be able to drop graph with admin privileges" );
	}

	public function testListGraphs() {
		global $gpTestGraphName;

		$gp = $this->newConnection();
		
		$graphs = $gp->capture_list_graphs();
		$graphs = array_column( $graphs, 0 );
		$this->assertTrue( in_array( $gpTestGraphName, $graphs ), "test table $gpTestGraphName should be in the list" );
		
		$this->gp->drop_graph($gpTestGraphName);
		$graphs = $gp->capture_list_graphs();
		print "graphs: " . var_export($graphs, true) . "\n";
		
		$graphs = array_column( $graphs, 0 );
		
		print "graphs: " . var_export($graphs, true) . "\n";
		
		print "containes: " . var_export(gpSlaveTest::setContains( $graphs, $gpTestGraphName ), true) . "\n";
		
		$this->assertFalse( gpSlaveTest::setContains( $graphs, $gpTestGraphName ), "test table $gpTestGraphName should no longer be in the list" );
	}

	public function testPipingPrivilege() {
		
	}

	public function testAddArcsPrivilege() {
		
	}

	public function testDeleteArcsPrivilege() {
		
	}

	public function testClearPrivilege() {
		
	}

	public function testReplaceSuccessorsPrivilege() {
		
	}

	public function testShutdown() {

	}


}

//TODO: (optionally) start server instance here! let it die when the test script dies.

//TODO: CLI interface behaviour of server (port config, etc)
