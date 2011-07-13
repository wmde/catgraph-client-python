<?php
require_once('gpTestBase.php');
 
/**
 * Tests a connection to a slave process, as well as general client lib functionality.
 */
class gpSlaveTest extends gpSlaveTestBase
{
    //// Client Lib Functions ///////////////////////////////////////////////////////////////
    //TODO: test getStatusMessage, isClosed, etc
    
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
		
		// two column data as map
		$a = $this->gp->capture_stats_map();
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
	
	public function dummyCallHandler($gp, &$cmd, &$args, &$source, &$sink, &$capture, &$result) {
		if ( $cmd == 'dummy' ) {
			$result = "test";
			return false;
		}
		
		return true;
	}
	
	public function testCallHandler() {
		$h = array( $this, 'dummyCallHandler' );
		$this->gp->addCallHandler($h);
		
		$st = $this->gp->capture_stats();
		$this->assertTrue(is_array($st), 'capture_stats is ecpedted to return an array!');
		
		$x = $this->gp->dummy();
		$this->assertEquals('test', $x);
	}

	private function assertCommandAccepted( $cmd, $src = null, $sink = null ) {
		$s = preg_replace('/\s+/s', ' ', var_export( $cmd, true ));
		
		try {
			$x = $this->gp->exec($cmd, $src, $sink);
			throw new Exception("dummy command should have failed in core: $s");
		} catch ( gpUsageException $e ) {
			$this->fail("command syntax should be accepted by client: $s");
		} catch ( gpProcessorException $e ) {
			//ok, should fail in core, but be accepted by client side validator
		}
	}
	
	private function assertCommandRejected( $cmd, $src = null, $sink = null ) {
		$s = preg_replace('/\s+/s', ' ', var_export( $cmd, true ));
		
		try {
			$x = $this->gp->exec($cmd, $src, $sink);
			$this->fail("bad command should be detected: $s");
		} catch ( gpUsageException $e ) {
			//ok
		} catch ( gpProcessorException $e ) {
			$this->fail("bad command should have been detected by the client: $s; core message: " . $e->getMessage());
		}
	}
	
	public function testCommandValidation() {
		$this->assertCommandRejected( '' );
		$this->assertCommandRejected( array('', 23) );
		
		$this->assertCommandRejected( null );
		$this->assertCommandRejected( array(null, 23) );
		
		$this->assertCommandRejected( false );
		$this->assertCommandRejected( array(false, 23) );

		$this->assertCommandRejected( array() );
		$this->assertCommandRejected( array( array('foo') ) );

		$this->assertCommandRejected( '123' );
		$this->assertCommandRejected( array('123') );
		
		$this->assertCommandAccepted( ' x ' );
		$this->assertCommandRejected( array(' x ') );
		
		$this->assertCommandRejected( '<x>y' );
		$this->assertCommandRejected( array(' <x>y ') );

		$this->assertCommandRejected( array('a:b') ); // 'a:b' is legal as an argument, but nut as a command name!
		
		$this->assertCommandAccepted( 'x' );
		$this->assertCommandAccepted( array('x') );
		
		$this->assertCommandAccepted( 'xyz' );
		$this->assertCommandAccepted( array('xyz') );
		
		$this->assertCommandAccepted( 'x7y' );
		$this->assertCommandAccepted( array('x7y') );
		
		$chars = "\r\n\t\0\x09^\"§\$%/()[]\{\}=?'`\\*+~.,;@\xDD";
		for ( $i = 0; $i<strlen($chars); $i++ ) {
			$ch = $chars[$i];
			$s = "a{$ch}b";
		
			$this->assertCommandRejected( $s );
			$this->assertCommandRejected( array($s) );
		}
		
		$chars = " !&<>|#:";
		for ( $i = 0; $i<strlen($chars); $i++ ) {
			$ch = $chars[$i];
			$s = "a{$ch}b";
		
			$this->assertCommandRejected( array($s) );
		}
		
		// operators -----------------------------------------
		$this->assertCommandAccepted( 'clear && clear' );
		$this->assertCommandAccepted( 'clear !&& clear' );

		
		// pipes disallowed -----------------------------------------
		$this->gp->allowPipes = false;

		$this->assertCommandRejected( 'clear > /tmp/test' );
		$this->assertCommandRejected( 'clear < /tmp/test' );
		
		// pipes allowed -----------------------------------------
		$this->gp->allowPipes = true;
		
		$this->assertCommandAccepted( 'clear > /tmp/test' );
		$this->assertCommandAccepted( 'clear < /tmp/test' );
		
		// pipes conflict -----------------------------------------
		$this->assertCommandRejected( 'clear > /tmp/test', null, gpNullSink::$instance );
		$this->assertCommandRejected( 'clear < /tmp/test', gpNullSource::$instance, null );
	}

	private function assertArgumentAccepted( $arg ) {
		$s = preg_replace('/\s+/s', ' ', var_export( $arg, true ));
		
		try {
			$x = $this->gp->exec( array('foo', $arg) );
			throw new Exception("dummy command should have failed in core: foo $s");
		} catch ( gpUsageException $e ) {
			$this->fail("argument should be accepted by client: $s");
		} catch ( gpProcessorException $e ) {
			//ok, should fail in core, but be accepted by client side validator
		}
	}
	
	private function assertArgumentRejected( $arg ) {
		$s = preg_replace('/\s+/s', ' ', var_export( $arg, true ));
		
		try {
			$x = $this->gp->exec( array('foo', $arg) );
			$this->fail("malformed argument should be detected: $s");
		} catch ( gpUsageException $e ) {
			//ok
		} catch ( gpProcessorException $e ) {
			$this->fail("malformed argument should have been detected by the client: $s; core message: " . $e->getMessage());
		}
	}
	
	public function testArgumentValidation() {
		$this->assertArgumentRejected( '' );
		$this->assertArgumentRejected( null );
		$this->assertArgumentRejected( false );
		$this->assertArgumentRejected( ' x ' );
		
		//$this->gp->setTimeout(2); // has no effect for pipes
		$this->assertArgumentAccepted( 'x:y' ); // needed for password auth! //NOTE: This is broken in graphcore (but works via graphserv)!
		
		$this->assertArgumentAccepted( '123' );
		$this->assertArgumentAccepted( 'x' );
		$this->assertArgumentAccepted( 'xyz' );
		$this->assertArgumentAccepted( 'x7y' );
		$this->assertArgumentAccepted( '7x7' );
		
		$chars = " \r\n\t\0\x09^!\"§\$%&/()[]\{\}=?'#`\\*+~.,;<>|@\xDD";
		for ( $i = 0; $i<strlen($chars); $i++ ) {
			$ch = $chars[$i];
			$s = "a{$ch}b";
		
			$this->assertArgumentRejected( $s );
		}
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
		$f = tempnam(sys_get_temp_dir(), 'gpt');
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

	public function testNullSource() {
		$this->gp->add_arcs( gpNullSource::$instance );
		$this->assertStatus( 'OK' );
	}
    
	public function testNullSink() {
		//generate output
		$this->gp->add_arcs( array(
			array( 1, 11 ),
			array( 1, 12 ),
			array( 11, 111 ),
			array( 11, 112 ),
		) );

		$ok = $this->gp->traverse_successors(1, 2, gpNullSink::$instance);
		$this->assertStatus('OK');
	}

    //// Slave Connection Tests ///////////////////////////////////////////////////
    // currently none. could check if the process really dies after quit, etc
    //TODO: test checkPeer, etc
}
