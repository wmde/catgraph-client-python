<?php

require_once('gpTestBase.php');
require_once('../gpMySQL.php');

error_reporting(E_ALL);

class gpMySQLTest extends gpSlaveTestBase {
	static $mysql;
	
	public function setUp() {
		global $gpTestMySQLHost, $gpTestMySQLUser, $gpTestMySQLPassword, $gpTestMySQLDatabase;
		
		parent::setUp();
		
		$this->mysql = gpMySQL::connect($gpTestMySQLHost, $gpTestMySQLUser, $gpTestMySQLPassword);
		$this->mysql->select_db($gpTestMySQLDatabase);
	}
		
	public function tearDown() {
		$this->mysql->close();

		parent::tearDown();
	}

    protected function makeTable( $table, $field1, $field2=null ) {
		$sql = "CREATE TEMPORARY TABLE IF NOT EXISTS " . $table; 
		$sql .= "(";
		$sql .= $field1 . " INT NOT NULL";
		if ($field2) $sql .= ", " . $field2 . " INT NOT NULL";
		$sql .= ")";
		
		$this->mysql->query($sql);
		
		$sql = "TRUNCATE TABLE $table";
		$this->mysql->query($sql);
	}
	
    public function testSource() {
        $this->makeTable( "test", "a", "b" );
        $this->mysql->query( "INSERT INTO test VALUES (3, 8)" );
        $this->mysql->query( "INSERT INTO test VALUES (7, 9)" );
        $this->mysql->query( "INSERT INTO test VALUES (11, 11)" );
        
		//-----------------------------------------------------------
        $src = $this->mysql->make_source( "test", "a", "b" );
        
        $this->assertTrue( gpTestBase::arrayEquals( array(3, 8), $src->nextRow() ), "expected row to be 3,8 " );
        $this->assertTrue( gpTestBase::arrayEquals( array(7, 9), $src->nextRow() ), "expected row to be 7,9" );
        $this->assertTrue( gpTestBase::arrayEquals( array(11, 11), $src->nextRow() ), "expected row to be 11,11" );
        $this->assertNull( $src->nextRow(), "expected next row to be null" );
        
        $src->close();

		//-----------------------------------------------------------
        $src = $this->mysql->make_query_source( "select a from test where a > 7", "a" );
        
        $this->assertTrue( gpTestBase::arrayEquals( array(11), $src->nextRow() ), "expected row to be 11" );
        $this->assertNull( $src->nextRow(), "expected next row to be null" );
        
        $src->close();
    }

    public function testTempSink() {
		$snk = $this->mysql->make_temp_sink( "a", "b" );
		$table = $snk->getTable();
		
		$snk->putRow( array(4,5) );
		$snk->putRow( array(6,7) );
		$snk->close();
		
		$res = $this->mysql->query( "SELECT a, b FROM $table ORDER BY a, b");
		
		$this->assertTrue( gpTestBase::arrayEquals( array('a' => 4, 'b' => 5), $r = $this->mysql->fetch_assoc($res) ), "expected row to be 4,5, got " . var_export($r, true) );
		$this->assertTrue( gpTestBase::arrayEquals( array('a' => 6, 'b' => 7), $r = $this->mysql->fetch_assoc($res) ), "expected row to be 6,7, got " . var_export($r, true) );
		$this->assertFalse(  $this->mysql->fetch_assoc($res), "expected next row to be false" );
		
		$this->mysql->free_result($res);
		
		$snk->drop();
    }

    public function testAddArcsFromSource() {
        $this->makeTable( "test", "a", "b" );
        $this->mysql->query( "INSERT INTO test VALUES (1, 11)" );
        $this->mysql->query( "INSERT INTO test VALUES (1, 12)" );
        $this->mysql->query( "INSERT INTO test VALUES (11, 111)" );
        $this->mysql->query( "INSERT INTO test VALUES (11, 112)" );
        
		//-----------------------------------------------------------
        $src = $this->mysql->make_source( "test", "a", "b" );
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
		$snk = $this->mysql->make_temp_sink( "n" );
        $src = $this->gp->traverse_successors( 1, 8, $snk );
        $snk->close();
        $table = $snk->getTable();
        
		$res = $this->mysql->query( "SELECT n FROM $table ORDER BY n");
		
		$this->assertTrue( gpTestBase::arrayEquals( array('n' => 1), $r = $this->mysql->fetch_assoc($res) ), "expected row to be 1 got " . var_export($r, true) );
		$this->assertTrue( gpTestBase::arrayEquals( array('n' => 11), $r = $this->mysql->fetch_assoc($res) ), "expected row to be 11, got " . var_export($r, true) );
		$this->assertTrue( gpTestBase::arrayEquals( array('n' => 12), $r = $this->mysql->fetch_assoc($res) ), "expected row to be 12, got " . var_export($r, true) );
		$this->assertTrue( gpTestBase::arrayEquals( array('n' => 111), $r = $this->mysql->fetch_assoc($res) ), "expected row to be 111, got " . var_export($r, true) );
		$this->assertTrue( gpTestBase::arrayEquals( array('n' => 112), $r = $this->mysql->fetch_assoc($res) ), "expected row to be 112, got " . var_export($r, true) );
		$this->assertFalse(  $this->mysql->fetch_assoc($res), "expected next row to be false" );
		
		$this->mysql->free_result($res);
    }

}

