<?php
require_once('gpTestBase.php');

$gpTestGraphName = 'test' . getmypid();
$gpTestFilePrefix = '/tmp/gptest-' . getmypid();

/**
 * Tests server functions via clinet lib
 */
class gpServerTest extends gpClientTestBase
{
	
	//// graph management functions ////////////////////////////////////////
	public function testCreateGraph() {
		global $gpTestGraphName;
		
		$name = $gpTestGraphName."_2";

		// create the graph
		$this->gp->create_graph($name);
		
		//make sure we can't create it twice
		$ok = $this->gp->try_create_graph($name);
		$this->assertFalse( $ok, "should not be able to create graph again when it already exists" );
		
		// see if we can use the graph from another connection
		$gp2 = $this->newConnection();
		
		$gp2->use_graph($name);

		// see if we can drop the graph while it's used
		$this->gp->drop_graph($name);
		
		//TODO: $gp2 should now reporterrors, because the grpah is gone. test that.

		// see if we can re-create the graph after it was dropped
		$this->gp->create_graph($name); 
		$this->gp->drop_graph($name);
		
		//TODO: test name restrictions
	}

	public function testCreateNameRestrictions() {
		global $gpTestGraphName;
		
		$this->gp->strictArguments = false; // disable strict client-side validation
		
		try {
			$n = '';
			$ok = $this->gp->create_graph($n);
			$this->fail("empty graph names should be forbidden!" );
		} catch ( gpException $ex ) {
			// ok
		}

		$n = '1337';
		$ok = $this->gp->try_create_graph($n);
		$this->assertFalse( $ok, "numeric graph names should be forbidden! (name: `$n`)" );

		$n = '1337' . $gpTestGraphName;
		$ok = $this->gp->try_create_graph($n);
		$this->assertFalse( $ok, "graph names starting with a number should be forbidden! (name: `$n`)" );

		$chars = " \r\n\t\0\x09^!\"§\$%&/()[]\{\}=?'#`\\*+~.:,;<>|@";
		for ( $i = 0; $i<strlen($chars); $i++ ) {
			$ch = $chars[$i];
			
			try {
				$n = $gpTestGraphName . $ch . "test";			
				$ok = $this->gp->create_graph($n);
				$this->fail("graph names containing `$ch` should be forbidden! (name: `$n`)" );
			} catch ( gpException $ex ) {
				// ok
			}

			try {
				$n = $ch . $gpTestGraphName;
				$ok = $this->gp->create_graph($n);
				$this->fail("graph names starting with  `$ch` should be forbidden! (name: `$n`)" );
			} catch ( gpException $ex ) {
				// ok
			}
		}

		$n = 'test1337' . $gpTestGraphName;
		$ok = $this->gp->try_create_graph($n);
		$this->assertEquals( 'OK', $ok, "graph names containing numbers should be allowd! (name: `$n`)" );
		$this->gp->try_drop_graph($n);
		
		$chars = '-_8';
		for ( $i = 0; $i<strlen($chars); $i++ ) {
			$ch = $chars[$i];
			
			$n = 'test' . $ch . $gpTestGraphName;
			$ok = $this->gp->try_create_graph($n);
			$this->assertEquals( 'OK', $ok, "graph names containing `".$ch."` should be allowd! (name: `$n`)" );
			$this->gp->try_drop_graph($n);
		}

	}

	public function testDropGraph() {
		global $gpTestGraphName;
		
		$name = $gpTestGraphName."_2";
		
		$this->gp->create_graph($name);
		$this->gp->drop_graph($name);
		
		$ok = $this->gp->try_use_graph($name);
		$this->assertFalse( $ok, "should not be able to use graph after dropping it" );
		
		$ok = $this->gp->try_drop_graph($name);
		$this->assertEquals( 'NONE', $ok, "should not be able to drop graph again after it was already dropped." );
	}

	public function testListGraphs() {
		global $gpTestGraphName;

		$gp = $this->newConnection();
		
		$graphs = $gp->capture_list_graphs();
		$graphs = array_column( $graphs, 0 );
		$this->assertTrue( in_array( $gpTestGraphName, $graphs ), "test table $gpTestGraphName should be in the list" );
		
		$this->gp->drop_graph($gpTestGraphName);
		$graphs = $gp->capture_list_graphs();
		#print "graphs: " . var_export($graphs, true) . "\n";
		
		$graphs = array_column( $graphs, 0 );
		
		#print "graphs: " . var_export($graphs, true) . "\n";
		
		#print "containes: " . var_export(gpConnectionTestBase::setContains( $graphs, $gpTestGraphName ), true) . "\n";
		
		$this->assertFalse( gpConnectionTestBase::setContains( $graphs, $gpTestGraphName ), "test table $gpTestGraphName should no longer be in the list" );
	}

	public function testShutdown() {

	}

	public function testQuit() {

	}

	//// privileges //////////////////////////////////////////////////////////
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

	public function testPipingPrivilege() {
		
	}

	public function testAddArcsPrivilege() {
		//TODO: remember to also test protocol re-sync after insertion was denied. server must slurp all data!
		
	}

	public function testDeleteArcsPrivilege() {
		
	}

	public function testClearPrivilege() {
		
	}

	public function testReplaceSuccessorsPrivilege() {
		
	}


}

//TODO: (optionally) start server instance here! let it die when the test script dies.

//TODO: CLI interface behaviour of server (port config, etc)
