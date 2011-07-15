<?php

require_once('../gpClient.php');

#require_once('PHPUnit/Framework.php');
require_once('gpTestConfig.php');

abstract class gpTestBase extends PHPUnit_Framework_TestCase
{
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

abstract class gpConnectionTestBase extends gpTestBase
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
		
		$this->assertEquals( $value, $stats[$field], "status[$field]" );
    }
    
    public function assertSessionValue($field, $value ) {
		$stats = $this->gp->capture_session_info();
		$stats = pairs2map( $stats );
		
		$this->assertEquals( $value, $stats[$field], "session_info[$field]" );
    }
    
    public function assertStatus($value, $mssage = null) {
		$status = $this->gp->getStatus();
		
		$this->assertEquals( $value, $status, $mssage );
    }
    
}

abstract class gpSlaveTestBase extends gpConnectionTestBase
{
	public function setUp() {
		global $gpTestGraphCorePath;
		
		$this->dump = new gpPipeSink( STDOUT ); 

		try {
			$this->gp = gpConnection::new_slave_connection( $gpTestGraphCorePath );
			$this->gp->connect();
		} catch ( gpException $ex ) {
			print("Unable to launch graphcore instance from $gpTestGraphCorePath, please make sure graphcore is installed and check the \$gpTestGraphCorePath configuration options in gpTestConfig.php.\nOriginal error: " . $ex->getMessage() . "\n");
			exit(10);
		}
	}
	
}

abstract class gpClientTestBase extends gpConnectionTestBase
{
	public function setUp() {
		global $gpTestAdmin, $gpTestAdminPassword;
		global $gpTestGraphName;
		global $gpTestGraphServHost, $gpTestGraphServPort;
		
		try {
			$this->gp = $this->newConnection(); 
		} catch ( gpException $ex ) {
			print("Unable to connect to $gpTestGraphServHost:$gpTestGraphServPort, please make sure the graphserv process is running and check the \$gpTestGraphServHost and \$gpTestGraphServPort configuration options in gpTestConfig.php.\nOriginal error: " . $ex->getMessage() . "\n");
			exit(11);
		}
		
		try {
			$this->gp->authorize( 'password', "$gpTestAdmin:$gpTestAdminPassword" );
		} catch ( gpException $ex ) {
			print("Unable to connect to authorize as $gpTestAdmin, please check the \$gpTestAdmin and \$gpTestAdminPassword configuration options in gpTestConfig.php.\nOriginal error: " . $ex->getMessage() . "\n");
			exit(12);
		}

		try {
			$this->gp->create_graph( $gpTestGraphName );
		} catch ( gpException $ex ) {
			print("Unable to create graphe $gpTestGraphName, please check the \$gpTestGraphName configuration option in gpTestConfig.php as well as the privileges of user $gpTestAdmin.\nOriginal error: " . $ex->getMessage() . "\n");
			exit(13);
		}

		$this->gp->use_graph( $gpTestGraphName );
		//if use_graph throws an error, let it rip. it really shouldn't happen and it's not a confiugration problem
	}
	
	public function newConnection() {
		global $gpTestGraphServHost, $gpTestGraphServPort;

		$gp = gpConnection::new_client_connection( null, $gpTestGraphServHost, $gpTestGraphServPort );
		$gp->connect();

		return $gp;
	}
	
	public function tearDown() {
		global $gpTestGraphName;
		global $gpTestAdmin, $gpTestAdminPassword;
		
		try {
			$this->gp->drop_graph( $gpTestGraphName );
		} catch ( gpProtocolException $ex ) {
			//failed to remove graph, maybe the connection is gone? try again.
			
			try {
				$gp = $this->newConnection(); 
				$gp->authorize( 'password', "$gpTestAdmin:$gpTestAdminPassword" );
				$gp->drop_graph( $gpTestGraphName );
			} catch ( gpException $ex ) {
				// just give up
			}
		}
		
		parent::tearDown();
	}

}
