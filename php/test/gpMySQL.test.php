<?php

require_once('gpTestBase.php');
require_once('../gpMySQL.php');

error_reporting(E_ALL);

class gpMySQLTest extends gpSlaveTestBase {
	static $mysql;
	
	public function setUp() {
		global $gpTestGraphCorePath;
		global $gpTestMySQLHost, $gpTestMySQLUser, $gpTestMySQLPassword, $gpTestMySQLDatabase;
		
		$this->dump = new gpPipeSink( STDOUT ); 

		try {
			$this->gp = gpMySQLGLue::new_slave_connection( $gpTestGraphCorePath );
			$this->gp->connect();
		} catch ( gpException $ex ) {
			print("Unable to launch graphcore instance from $gpTestGraphCorePath, please make sure graphcore is installed and check the \$gpTestGraphCorePath configuration options in gpTestConfig.php.\nOriginal error: " . $ex->getMessage() . "\n");
			exit(10);
		}

		try {
			$this->gp->mysql_connect($gpTestMySQLHost, $gpTestMySQLUser, $gpTestMySQLPassword);
			$this->gp->mysql_select_db($gpTestMySQLDatabase);
		} catch ( gpException $ex ) {
			print("Unable to connect to table $gpTestMySQLDatabase on MySQL host $gpTestMySQLHost as $gpTestMySQLUser, please make sure MySQL is running and check the \$gpTestMySQLHost and related configuration options in gpTestConfig.php.\nOriginal error: " . $ex->getMessage() . "\n");
			exit(10);
		}
	}
	
    protected function makeTable( $table, $field1, $field2=null ) {
		$sql = "CREATE TEMPORARY TABLE IF NOT EXISTS " . $table; 
		$sql .= "(";
		$sql .= $field1 . " INT NOT NULL";
		if ($field2) $sql .= ", " . $field2 . " INT NOT NULL";
		$sql .= ")";
		
		$this->gp->mysql_query($sql);
		
		$sql = "TRUNCATE TABLE $table";
		$this->gp->mysql_query($sql);
	}
	
    public function testSource() {
        $this->makeTable( "test", "a", "b" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (3, 8)" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (7, 9)" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (11, 11)" );
        
		//-----------------------------------------------------------
        $src = $this->gp->make_source( "test", "a", "b" );
        
        $this->assertTrue( gpTestBase::arrayEquals( array(3, 8), $src->nextRow() ), "expected row to be 3,8 " );
        $this->assertTrue( gpTestBase::arrayEquals( array(7, 9), $src->nextRow() ), "expected row to be 7,9" );
        $this->assertTrue( gpTestBase::arrayEquals( array(11, 11), $src->nextRow() ), "expected row to be 11,11" );
        $this->assertNull( $src->nextRow(), "expected next row to be null" );
        
        $src->close();

		//-----------------------------------------------------------
        $src = $this->gp->make_source( "select a from test where a > 7", "a" );
        
        $this->assertTrue( gpTestBase::arrayEquals( array(11), $src->nextRow() ), "expected row to be 11" );
        $this->assertNull( $src->nextRow(), "expected next row to be null" );
        
        $src->close();
    }

    public function testTempSink() {
		$snk = $this->gp->make_temp_sink( "a", "b" );
		$table = $snk->getTable();
		
		$snk->putRow( array(4,5) );
		$snk->putRow( array(6,7) );
		$snk->close();
		
		$res = $this->gp->mysql_query( "SELECT a, b FROM $table ORDER BY a, b");
		
		$this->assertTrue( gpTestBase::arrayEquals( array('a' => 4, 'b' => 5), $r = $this->gp->mysql_fetch_assoc($res) ), "expected row to be 4,5, got " . var_export($r, true) );
		$this->assertTrue( gpTestBase::arrayEquals( array('a' => 6, 'b' => 7), $r = $this->gp->mysql_fetch_assoc($res) ), "expected row to be 6,7, got " . var_export($r, true) );
		$this->assertFalse(  $this->gp->mysql_fetch_assoc($res), "expected next row to be false" );
		
		$this->gp->mysql_free_result($res);
		
		$snk->drop();
    }

    public function testAddArcsFromSource() {
        $this->makeTable( "test", "a", "b" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (1, 11)" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (1, 12)" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (11, 111)" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (11, 112)" );
        
		//-----------------------------------------------------------
        $src = $this->gp->make_source( "test", "a", "b" );
        $this->gp->add_arcs( $src );
		$src->close();
		
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

    public function testSuccessorsToSink() {
		$this->gp->add_arcs( array(
			array( 1, 11 ),
			array( 1, 12 ),
			array( 11, 111 ),
			array( 11, 112 ),
		) );
        
		//-----------------------------------------------------------
		$snk = $this->gp->make_temp_sink( "n" );
        $src = $this->gp->traverse_successors( 1, 8, $snk );
        $snk->close();
        $table = $snk->getTable();
        
		$res = $this->gp->mysql_query( "SELECT n FROM $table ORDER BY n");
		
		$this->assertTrue( gpTestBase::arrayEquals( array('n' => 1), $r = $this->gp->mysql_fetch_assoc($res) ), "expected row to be 1 got " . var_export($r, true) );
		$this->assertTrue( gpTestBase::arrayEquals( array('n' => 11), $r = $this->gp->mysql_fetch_assoc($res) ), "expected row to be 11, got " . var_export($r, true) );
		$this->assertTrue( gpTestBase::arrayEquals( array('n' => 12), $r = $this->gp->mysql_fetch_assoc($res) ), "expected row to be 12, got " . var_export($r, true) );
		$this->assertTrue( gpTestBase::arrayEquals( array('n' => 111), $r = $this->gp->mysql_fetch_assoc($res) ), "expected row to be 111, got " . var_export($r, true) );
		$this->assertTrue( gpTestBase::arrayEquals( array('n' => 112), $r = $this->gp->mysql_fetch_assoc($res) ), "expected row to be 112, got " . var_export($r, true) );
		$this->assertFalse(  $this->gp->mysql_fetch_assoc($res), "expected next row to be false" );
		
		$this->gp->mysql_free_result($res);
    }

    public function testSuccessorsToSinkShorthand() {
		$this->gp->add_arcs( array(
			array( 1, 11 ),
			array( 1, 12 ),
			array( 11, 111 ),
			array( 11, 112 ),
		) );
        
		//-----------------------------------------------------------
        $snk = $this->gp->traverse_successors_into( 1, 8, "? x y" );
        $snk->close();
        $table = $snk->getTable();
        
		$res = $this->gp->mysql_query( "SELECT n FROM $table ORDER BY n");
		
		$this->assertTrue( gpTestBase::arrayEquals( array('n' => 1), $r = $this->gp->mysql_fetch_assoc($res) ), "expected row to be 1 got " . var_export($r, true) );
		$this->assertTrue( gpTestBase::arrayEquals( array('n' => 11), $r = $this->gp->mysql_fetch_assoc($res) ), "expected row to be 11, got " . var_export($r, true) );
		$this->assertTrue( gpTestBase::arrayEquals( array('n' => 12), $r = $this->gp->mysql_fetch_assoc($res) ), "expected row to be 12, got " . var_export($r, true) );
		$this->assertTrue( gpTestBase::arrayEquals( array('n' => 111), $r = $this->gp->mysql_fetch_assoc($res) ), "expected row to be 111, got " . var_export($r, true) );
		$this->assertTrue( gpTestBase::arrayEquals( array('n' => 112), $r = $this->gp->mysql_fetch_assoc($res) ), "expected row to be 112, got " . var_export($r, true) );
		$this->assertFalse(  $this->gp->mysql_fetch_assoc($res), "expected next row to be false" );
		
		$this->gp->mysql_free_result($res);
    }

}

