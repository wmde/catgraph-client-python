<?php

require_once 'PHPUnit/Framework.php';
require_once('gpClient.php');
 
class gpClientTest extends PHPUnit_Framework_TestCase
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

    public function testPing() {
		$pong = $this->gp->ping();
		print "PONG: $pong\n";
    }
    
    public function testStats() {
		$stats = $this->gp->stats(GP_CAPTURE);
		$stats = gpClientTest::pairs2map( $stats );
		
		$this->assertEquals( $stats['ArcCount'], 0 );
    }
    
    public function assertStatus($field, $value) {
		$stats = $this->gp->stats(GP_CAPTURE);
		$stats = gpClientTest::pairs2map( $stats );
		
		$this->assertEquals( $stats[$field], $value );
    }
    
    public static function pairs2map( $pairs ) {
		$map = array();
		
		foreach ( $pairs as $p ) {
			$map[ $p[0] ] = $p[1];
		}
		
		return $map;
	}
}

$gpTestGraphCorePath = '/home/daniel/src/graphserv/graphcore/graphcore';

