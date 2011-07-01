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

		$gp2 = $this->newConnection();
		
		$graphs = $gp2->capture_list_graphs();
		$graphs = array_column( $graphs, 0 );
		$this->assertTrue( in_array( $gpTestGraphName, $graphs ), "test table $gpTestGraphName should be in the list" );
		
		$this->gp->drop_graph($gpTestGraphName);
		$graphs = $gp2->capture_list_graphs();
		#print "graphs: " . var_export($graphs, true) . "\n";
		
		$graphs = array_column( $graphs, 0 );
		
		#print "graphs: " . var_export($graphs, true) . "\n";
		
		#print "containes: " . var_export(gpConnectionTestBase::setContains( $graphs, $gpTestGraphName ), true) . "\n";
		
		$this->assertFalse( gpConnectionTestBase::setContains( $graphs, $gpTestGraphName ), "test table $gpTestGraphName should no longer be in the list" );
	}

	public function testShutdown() {
		global $gpTestGraphName;
		
		$gp2 = $this->newConnection();
		$gp2->use_graph($gpTestGraphName);
		$gp2->stats();

		$this->assertSessionValue('ConnectedGraph', $gpTestGraphName);
		
		$this->gp->shutdown(); // <------------------
		//$this->assertSessionValue('ConnectedGraph', 'None'); //nice, but not reliable. race condition.
		
		$this->gp->try_stats();
		$this->assertEquals( 'FAILED', $this->gp->getStatus(), 'fetching stats should fail after shutdown' );
		
		$gp2->try_stats();
		$this->assertEquals( 'FAILED', $gp2->getStatus(), 'fetching stats should fail after shutdown' );
		$gp2->close();
		
		$gp3 = $this->newConnection();
		$gp3->try_use_graph($gpTestGraphName);
		$this->assertEquals( 'FAILED', $gp3->getStatus(), 'graph should be unavailable after shutdown' );
		$gp3->close();
	}

	public function testQuit() {
		global $gpTestGraphName;
		
		$gp2 = $this->newConnection();
		$gp2->use_graph($gpTestGraphName);
		$gp2->stats();

		$this->assertSessionValue('ConnectedGraph', $gpTestGraphName);
		
		$this->gp->quit();  // <------------------
		$this->assertStatus('OK');
		
		try {
			$this->gp->try_stats();
			$this->fail( 'connection should be unusable after quit' );
		} catch ( gpProtocolException $e ) {
			//ok
		}
		
		$gp2->stats();
		$this->assertEquals( 'OK', $gp2->getStatus(), 'connection should still be usable by others after quit.' );
		$gp2->close();
		
		$gp3 = $this->newConnection();
		$gp3->use_graph($gpTestGraphName);
		$this->assertEquals( 'OK', $gp3->getStatus(), 'graph should still be available to others after quit.' );
		$gp3->close();
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

	public function testInputPipingPrivilege() {
		global $gpTestGraphName, $gpTestGraphServHost;
		global $gpTestAdmin, $gpTestAdminPassword;
		global $gpTestMaster, $gpTestMasterPassword;
		
		//XXX: this uses local files, so it will always fail if the server isn't on localhost!
		if ( $gpTestGraphServHost != 'localhost' ) return; 
		
		$f = dirname(__FILE__) . '/gp.test.data';
		
		$gp = $this->newConnection();
		$gp->use_graph($gpTestGraphName);
		$gp->allowPipes = true;
		
		$gp->authorize('password', "$gpTestMaster:$gpTestMasterPassword");
		
		try {
			$ok = $gp->exec("add-arcs < $f"); 
			$this->fail( "should not be able to pipe without admin privileges!" );
		} catch ( gpProcessorException $ex ) {
			$this->assertEquals( 'DENIED', $gp->getStatus(), "piping should be denied, not fail. Message: " . $ex->getMessage() );
		}

		$gp->authorize('password', "$gpTestAdmin:$gpTestAdminPassword"); // re-authenticate
		$ok = $gp->exec("add-arcs < $f"); 
		$this->assertEquals( $ok, 'OK', "should be able to pipe with admin privileges" );
	}

	public function testOutputPipingPrivilege() {
		global $gpTestGraphName, $gpTestGraphServHost;
		global $gpTestAdmin, $gpTestAdminPassword;
		global $gpTestMaster, $gpTestMasterPassword;
		
		//XXX: this uses local files, so it will always fail if the server isn't on localhost!
		if ( $gpTestGraphServHost != 'localhost' ) return; 
		
		$f = tempnam(sys_get_temp_dir(), 'gpt');
		
		$gp = $this->newConnection();
		$gp->use_graph($gpTestGraphName);
		$gp->allowPipes = true;
		
		try {
			$ok = $gp->exec("list-roots > $f"); 
			$this->fail( "should not be able to pipe without admin privileges!" );
		} catch ( gpProcessorException $ex ) {
			$this->assertEquals( 'DENIED', $gp->getStatus(), "piping should be denied, not fail. Message: " . $ex->getMessage() );
		}

		$gp->authorize('password', "$gpTestAdmin:$gpTestAdminPassword"); // re-authenticate
		$ok = $gp->exec("list-roots > $f"); 
		$this->assertEquals( $ok, 'OK', "should be able to pipe with admin privileges" );
		
		@unlink($f); //cleanup
	}

	public function testAddArcsPrivilege() {
		global $gpTestGraphName;
		global $gpTestMaster, $gpTestMasterPassword;
		
		$gp = $this->newConnection();
		$gp->use_graph($gpTestGraphName);
		
		$ok = $gp->try_add_arcs( array( array( 1, 11 ), array( 1, 12 ) ) );
		$this->assertFalse( $ok, "should not be able to add arcs without authorizing" );
		$this->assertEquals( 'DENIED', $gp->getStatus(), "command should be denied, not fail" );

		$gp->authorize('password', "$gpTestMaster:$gpTestMasterPassword");
		$ok = $gp->try_add_arcs( array( array( 1, 11 ), array( 1, 12 ) ) );
		$this->assertEquals( 'OK', $ok, "should be able to add arcs with updater privileges" );
	}

	public function testRemoveArcsPrivilege() {
		global $gpTestGraphName;
		global $gpTestMaster, $gpTestMasterPassword;

		$this->gp->add_arcs( array( array( 1, 11 ),	array( 1, 12 ) ) ); //add some arcs as admin
		
		$gp = $this->newConnection();
		$gp->use_graph($gpTestGraphName);
		
		$ok = $gp->try_remove_arcs( array( array( 1, 11 ) ) );
		$this->assertFalse( $ok, "should not be able to delete arcs without authorizing" );
		$this->assertEquals( 'DENIED', $gp->getStatus(), "command should be denied, not fail" );

		$gp->authorize('password', "$gpTestMaster:$gpTestMasterPassword");
		$ok = $gp->try_remove_arcs( array( array( 1, 11 ) ) );
		$this->assertEquals( 'OK', $ok, "should be able to delete arcs with updater privileges" );
	}

	public function testReplaceSuccessorsPrivilege() {
		global $gpTestGraphName;
		global $gpTestMaster, $gpTestMasterPassword;

		$this->gp->add_arcs( array( array( 1, 11 ),	array( 1, 12 ) ) ); //add some arcs as admin
		
		$gp = $this->newConnection();
		$gp->use_graph($gpTestGraphName);
		
		$ok = $gp->try_replace_successors( 1, array( 17 ) );
		$this->assertFalse( $ok, "should not be able to replace arcs without authorizing" );
		$this->assertEquals( 'DENIED', $gp->getStatus(), "command should be denied, not fail" );

		$gp->authorize('password', "$gpTestMaster:$gpTestMasterPassword");
		$ok = $gp->try_replace_successors( 1, array( 17 ) );
		$this->assertEquals( 'OK', $ok, "should be able to replace arcs with updater privileges" );
	}

	public function testReplacePredecessorsPrivilege() {
		global $gpTestGraphName;
		global $gpTestMaster, $gpTestMasterPassword;

		$this->gp->add_arcs( array( array( 1, 11 ),	array( 1, 12 ) ) ); //add some arcs as admin

		$gp = $this->newConnection();
		$gp->use_graph($gpTestGraphName);
		
		$ok = $gp->try_replace_predecessors( 1, array( 17 ) );
		$this->assertFalse( $ok, "should not be able to replace arcs without authorizing" );
		$this->assertEquals( 'DENIED', $gp->getStatus(), "command should be denied, not fail" );

		$gp->authorize('password', "$gpTestMaster:$gpTestMasterPassword");
		$ok = $gp->try_replace_predecessors( 1, array( 17 ) );
		$this->assertEquals( 'OK', $ok, "should be able to replace arcs with updater privileges" );
	}

	public function testClearPrivilege() {
		global $gpTestGraphName;
		global $gpTestAdmin, $gpTestAdminPassword;
		global $gpTestMaster, $gpTestMasterPassword;
		
		$gp = $this->newConnection();
		$gp->use_graph($gpTestGraphName);
		
		$ok = $gp->try_clear();
		$this->assertFalse( $ok, "should not be able to clear a graph without authorizing" );

		$gp->authorize('password', "$gpTestMaster:$gpTestMasterPassword");
		$ok = $gp->try_clear();
		$this->assertEquals( $ok, 'OK', "should be able to clear graph with updater privileges" );

		$gp->authorize('password', "$gpTestAdmin:$gpTestAdminPassword"); // re-authenticate
		$ok = $gp->try_clear();
		$this->assertEquals( $ok, 'OK', "should be able to clear graph with admin privileges" );
	}

	public function testShutdownPrivilege() {
		global $gpTestGraphName;
		global $gpTestAdmin, $gpTestAdminPassword;
		global $gpTestMaster, $gpTestMasterPassword;
		
		$gp = $this->newConnection();
		$gp->use_graph($gpTestGraphName);
		
		$ok = $gp->try_shutdown();
		$this->assertFalse( $ok, "should not be able to shut down a graph without authorizing" );

		$gp->authorize('password', "$gpTestMaster:$gpTestMasterPassword");
		$ok = $gp->try_shutdown();
		$this->assertFalse( $ok, "should not be able to shut down a graph without admin privileges" );

		$gp->authorize('password', "$gpTestAdmin:$gpTestAdminPassword"); // re-authenticate
		$ok = $gp->try_shutdown();
		$this->assertEquals( $ok, 'OK', "should be able to shut down graph with admin privileges" );
	}


}

//TODO: (optionally) start server instance here! let it die when the test script dies.

//TODO: CLI interface behaviour of server (port config, etc)
