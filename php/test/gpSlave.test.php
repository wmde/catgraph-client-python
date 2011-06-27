<?php

require_once('PHPUnit/Framework.php');
require_once('gpClient.php');
 
class gpSlaveTest extends PHPUnit_Framework_TestCase
{
	public function setUp() {
		global $gpTestGraphCorePath;
		
		$this->dump = new gpPipeSink( STDOUT ); 

		$this->gp = new gpSlave( $gpTestGraphCorePath );
		#$this->gp->debug = true;

		$this->gp->connect();
	}
	
	public function tearDown() {
		if ($this->gp) $this->gp->close();
	}
    ///////////////////////////////////////////////////////////////////

    public function testPing() {
		$pong = $this->gp->ping();
    }
    
    public function testStats() {
		$stats = $this->gp->capture_stats();
		$stats = pairs2map( $stats );
		
		$this->assertEquals( $stats['ArcCount'], 0, "arc count should be zero" );
    }
    
    public function testAddArcs() {
		$this->gp->add_arcs( array(
			array( 1, 11 ),
			array( 1, 12 ),
			array( 11, 111 ),
			array( 11, 112 ),
		) );
		
		$this->assertStatsValue( 'ArcCount', 4 );
		
		$arcs = $this->gp->capture_list_successors( 1 );
		
		$this->assertTrue( gpSlaveTest::setEquals( $arcs, array(
			array( 11 ),
			array( 12 ),
		) ), "sucessors of (1)" );
		
		$arcs = $this->gp->capture_list_successors( 11 );
		$this->assertTrue( gpSlaveTest::setEquals( $arcs, array(
			array( 111 ),
			array( 112 ),
		) ), "sucessors of (2)" );

		// ------------------------------------------------------
		
		$this->gp->add_arcs( array(
			array( 1, 11 ),
			array( 11, 112 ),
			array( 2, 21 ),
		) );

		$this->assertStatsValue( 'ArcCount', 5 );

		$arcs = $this->gp->capture_list_successors( 2 );
		$this->assertTrue( gpSlaveTest::setEquals( $arcs, array(
			array( 21 ),
		) ), "sucessors of (2)" );
		
    }
    
    ///////////////////////////////////////////////////////////////////
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
    
	public function testQuit() {
		$this->gp->quit();
		$this->assertStatus( 'OK' );

		try {
			$x = $this->gp->try_status();
			$this->fail("running a command on a closed connection should always trigger an error");
		} catch ( gpProtocolException $e ) {
			//this is the expected outcome: the connection is closed.
		}
	}

    ///////////////////////////////////////////////////////////////////
	public function testArraySource() {
		//TODO...
	}
    
	public function testArraySink() {
		//TODO...
	}
    
	public function testFileSource() {
		$f = dirname(__FILE__) . '/gp.test.data';
		$src = new gpFileSource($f);
	
		$this->gp->add_arcs( $src );

		$this->assertStatus( 'OK' );
		$this->assertStatsValue( 'ArcCount', 4 );
		
		$arcs = $this->gp->capture_list_successors( 1 );
		
		$this->assertTrue( gpSlaveTest::setEquals( $arcs, array(
			array( 11 ),
			array( 12 ),
		) ), "sucessors of (1)" );
		
		$arcs = $this->gp->capture_list_successors( 11 );
		$this->assertTrue( gpSlaveTest::setEquals( $arcs, array(
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
		$this->assertTrue( gpSlaveTest::setEquals($expected, $rows), 'bad content in outfile: ' . var_export( $rows, true ) . ', expected ' . var_export( $expected, true ) );

		//cleanup
		@unlink($f);
	}
    
    ///////////////////////////////////////////////////////////////////
    
    //TODO: test all commands, including edge cases. iuse talkback tests as the basis
	//      ...put that into gpCore.test.php instead...
    
    ///////////////////////////////////////////////////////////////////
    public function assertStatsValue($field, $value ) {
		$stats = $this->gp->capture_stats();
		$stats = pairs2map( $stats );
		
		$this->assertEquals( $stats[$field], $value, "status[$field]" );
    }
    
    public function assertStatus($value, $mssage = null) {
		$status = $this->gp->getStatus();
		
		$this->assertEquals( $value, $status, $mssage );
    }
    
    public static function setContains( $a, $w ) {
		$found = false;
		foreach ( $a as $v ) {
			if ( is_array( $v ) ) {
				if ( is_array( $w ) ) {
					if ( gpSlaveTest::arrayEquals( $v, $w ) ) {
						$found = true;
						break;
					}
				}
			} else {
				if ( $v == $w ) {
					$found = true;
					break;
				}
			}
		}
		
		return $found;
	}
	
    public static function setEquals( $a, $b ) {
		if ( count($a) != count($b) ) return false;
		
		foreach ( $a as $v ) {
			if ( !gpSlaveTest::setContains($b, $v) ) {
				return false;
			}
		}
		
		return true;
	}
    
    public static function arrayEquals( $a, $b ) {
		if ( count($a) != count($b) ) return false;
		
		foreach ( $a as $k => $v ) {
			$w = $b[$k];
			
			if ( is_array( $v ) ) {
				//WARNING: no protection against circular array references
				if ( !is_array($w) || !gpSlaveTest::arrayEquals( $w, $v ) ) {
					return false;
				}
			} else if ( $w != $v ) {
				return false;
			}
		}
		
		return true;
	}    
}

$gpTestGraphCorePath = '/home/daniel/src/graphserv/graphcore/graphcore'; #FIXME: hardcoded path

#$gpTestGraphCorePath = 'tee /tmp/graphcore.in | /home/daniel/src/graphserv/graphcore/graphcore | tee /tmp/graphcore.out';
