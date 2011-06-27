<?php
require_once('gpTestBase.php');
 
/**
 * Tests a connection to a slave process, as well as general client lib functionality.
 */
class gpSlaveTest extends gpSlaveTestBase
{
    //// Client Lib Functionality ///////////////////////////////////////////////////////////////
    // Tested here, not in gpConnectionTestBase, because we only need to test is once, not for every type of connection
    
    public function testTry() {
		$x = $this->gp->try_foo();

		$this->assertFalse( $x );
		$this->assertEquals( 'FAILED', $this->gp->getStatus() );
    }    
    
	public function testCapture() {
		// empty data
		$a = $this->gp->capture_list_roots();
		$this->assertStatus( 'OK' );
		$this->assertNotNull( $a );
		$this->assertType( 'array', $a );
		
		$this->assertEquals( 0, count($a), "number of items in the result should be 0!" );
		
		// single column data
		$this->gp->add_arcs( array(
			array( 1, 11 ),
			array( 1, 12 ),
			array( 11, 111 ),
			array( 11, 112 ),
		) );

		$a = $this->gp->capture_list_successors( 1 );
		$this->assertStatus( 'OK' );
		$this->assertNotNull( $a );
		$this->assertType( 'array', $a );
		
		$a = array_column( $a, 0 );
		$this->assertTrue( $this->arrayEquals( array(11, 12), $a ), "unexpected response for list_successors(1): " . var_export($a, true) );

		// two column data
		$a = $this->gp->capture_stats();
		$this->assertNotNull( $a );

		$a = pairs2map( $a );
		$this->assertArrayHasKey( 'ArcCount', $a, "contents: " . var_export($a, true) );
		$this->assertEquals( $a['ArcCount'], 4 );
		
		//capture none
		$a = $this->gp->capture_traverse_successors( 77, 5 );
		$this->assertStatus( 'NONE' );
		$this->assertNull( $a );
		
		//capture on command with no output
		$a = $this->gp->capture_clear();
		$this->assertStatus( 'OK' );
		$this->assertTrue( $a );
		
		//capture throwing error
		try {
			$x = $this->gp->capture_foo();
			$this->fail("capturing output of an unknown command should trigger an error");
		} catch ( gpProcessorException $e ) {
			//this is the expected outcome: the connection is closed.
		}
		
		//capture with try
		$x = $this->gp->try_capture_foo();
		//should not trigger an exception...
	}

    //// Client Lib I/O ///////////////////////////////////////////////////////////////
    // Tested here, not in gpConnectionTestBase, because we only need to test is once, not for every type of connection
	// Note: ArraySource and ArraySink are used implicitly all the time in the tests, no need to test them separately.
    
	public function testFileSource() {
		$f = dirname(__FILE__) . '/gp.test.data';
		$src = new gpFileSource($f);
	
		$this->gp->add_arcs( $src );

		$this->assertStatus( 'OK' );
		$this->assertStatsValue( 'ArcCount', 4 );
		
		$arcs = $this->gp->capture_list_successors( 1 );
		
		$this->assertTrue( gpConnectionTestBase::setEquals( $arcs, array(
			array( 11 ),
			array( 12 ),
		) ), "sucessors of (1)" );
		
		$arcs = $this->gp->capture_list_successors( 11 );
		$this->assertTrue( gpConnectionTestBase::setEquals( $arcs, array(
			array( 111 ),
			array( 112 ),
		) ), "sucessors of (2)" );
	}
    
	public function testFileSink() {
		
		//set up the sink
		$f = tempnam(sys_get_temp_dir(), '-gp.test.data');
		$sink = new gpFileSink($f);
		
		//generate output
		$this->gp->add_arcs( array(
			array( 1, 11 ),
			array( 1, 12 ),
			array( 11, 111 ),
			array( 11, 112 ),
		) );

		$ok = $this->gp->traverse_successors(1, 2, $sink);
		$sink->close();
		
		//make sure we can read the file
		$this->assertStatus('OK');
		$this->assertEquals('OK', $ok);
		
		//compare actual file contents
		$rows = file($f);
		$this->assertNotEquals(false, $rows, "could not get file contents of $f");
		$this->assertNotNull($rows, "could not get file contents of $f");
		
		$expected = array(
			"1\n",
			"11\n",
			"12\n",
			"111\n",
			"112\n",
		);
		$this->assertTrue( gpConnectionTestBase::setEquals($expected, $rows), 'bad content in outfile: ' . var_export( $rows, true ) . ', expected ' . var_export( $expected, true ) );

		//cleanup
		@unlink($f);
	}

    //// Slave Connection Tests ///////////////////////////////////////////////////
    // currently none. could check if the process really dies after quit, etc
       
}

