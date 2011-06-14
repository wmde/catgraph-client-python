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
		$stats = gpSlaveTest::pairs2map( $stats );
		
		$this->assertEquals( $stats['ArcCount'], 0, "arc count should be zero" );
    }
    
    public function testAddArcs() {
		$this->gp->add_arcs( array(
			array( 1, 11 ),
			array( 1, 12 ),
			array( 11, 111 ),
			array( 11, 112 ),
		) );
		
		$this->assertStatus( 'ArcCount', 4 );
		
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

		$this->assertStatus( 'ArcCount', 5 );

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
		//TODO: capture single column data
		//TODO: capture double column data
		//TODO: capture none
		//TODO: capture on command with no output (should be null or empty array?)
		//TODO: capture throwing error
		//TODO: capture with try
	}
    
	public function testShutdown() {
		//TODO...
	}

    ///////////////////////////////////////////////////////////////////
	public function testArraySource() {
		//TODO...
	}
    
	public function testFileSource() {
		//TODO...
	}
    
	public function testArraySink() {
		//TODO...
	}
    
	public function testFileSink() {
		//TODO...
	}
    
    ///////////////////////////////////////////////////////////////////
    public static function array_column($a, $col) {
		$column = array();
		
		foreach ( $a as $k => $x ) {
			$column[$k] = $x[$col];
		}
		
		return $column;
	}
	
    public function assertStatus($field, $value) {
		$stats = $this->gp->capture_stats();
		$stats = gpSlaveTest::pairs2map( $stats );
		
		$this->assertEquals( $stats[$field], $value, "status[$field]" );
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
    
    public static function pairs2map( $pairs ) {
		$map = array();
		
		foreach ( $pairs as $p ) {
			$map[ $p[0] ] = $p[1];
		}
		
		return $map;
	}
}

$gpTestGraphCorePath = '/home/daniel/src/graphserv/graphcore/graphcore'; #FIXME: hardcoded path

#$gpTestGraphCorePath = 'tee /tmp/graphcore.in | /home/daniel/src/graphserv/graphcore/graphcore | tee /tmp/graphcore.out';
