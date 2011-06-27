<?php

require_once('../gpClient.php');

#require_once('PHPUnit/Framework.php');
require_once('gpTestConfig.php');

abstract class gpConnectionTestBase extends PHPUnit_Framework_TestCase
{
	public function setUp() {
		throw new Exception('subclasses must override setUp() to store a gpConnection in $this->gp');
	}
		
	public function tearDown() {
		if (!empty($this->gp)) $this->gp->close();
	}

    //// Basic Connection Tests //////////////////////////////////////////////////
    // These need to pass for all types of connections
    // Note: lib functionality like try and capture is tested in gpSlaveTest,
    //       because it doesn't need to be tested separately for all types of connections

    public function testPing() {
		$pong = $this->gp->ping();
    }
    
    public function testStats() {
		$stats = $this->gp->capture_stats();
		$stats = pairs2map( $stats );
		
		$this->assertEquals( $stats['ArcCount'], 0, "arc count should be zero" );
    }
    
    public function testDataSetHandling() {
		$this->gp->add_arcs( array(
			array( 1, 11 ),
			array( 1, 12 ),
			array( 11, 111 ),
			array( 11, 112 ),
		) );
		
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
        
    ///// utility //////////////////////////////////////////////////////////////
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
					if ( gpConnectionTestBase::arrayEquals( $v, $w ) ) {
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
			if ( !gpConnectionTestBase::setContains($b, $v) ) {
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
				if ( !is_array($w) || !gpConnectionTestBase::arrayEquals( $w, $v ) ) {
					return false;
				}
			} else if ( $w != $v ) {
				return false;
			}
		}
		
		return true;
	}    
}

abstract class gpSlaveTestBase extends gpConnectionTestBase
{
	public function setUp() {
		global $gpTestGraphCorePath;
		
		$this->dump = new gpPipeSink( STDOUT ); 

		$this->gp = new gpSlave( $gpTestGraphCorePath );
		#$this->gp->debug = true;

		$this->gp->connect();
	}
	
}

abstract class gpClientTestBase extends gpConnectionTestBase
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

}
