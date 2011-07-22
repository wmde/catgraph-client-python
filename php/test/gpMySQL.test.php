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
			$this->gp = gpMySQLGlue::new_slave_connection( $gpTestGraphCorePath );
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
	
    protected function makeTable( $table, $fieldSpec ) {
		$sql = "CREATE TEMPORARY TABLE IF NOT EXISTS " . $table; 
		$sql .= "(";
		$sql .= $fieldSpec;
		$sql .= ")";
		
		$this->gp->mysql_query($sql);
		
		$sql = "TRUNCATE TABLE $table";
		$this->gp->mysql_query($sql);
	}
	
    public function testSource() {
        $this->makeTable( "test", "a INT NOT NULL, b INT NOT NULL" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (3, 8)" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (7, 9)" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (11, 11)" );
        
		//-----------------------------------------------------------
        $src = $this->gp->make_source( new gpMySQLTable("test", "a", "b") );
        
        $this->assertEquals(array(3, 8), $src->nextRow() , "expected row to be 3,8 " );
        $this->assertEquals(array(7, 9), $src->nextRow() , "expected row to be 7,9" );
        $this->assertEquals(array(11, 11), $src->nextRow() , "expected row to be 11,11" );
        $this->assertNull( $src->nextRow(), "expected next row to be null" );
        
        $src->close();

		//-----------------------------------------------------------
        $src = $this->gp->make_source( new gpMySQLSelect("select a from test where a > 7") );
        
        $this->assertEquals(array(11), $src->nextRow() , "expected row to be 11" );
        $this->assertNull( $src->nextRow(), "expected next row to be null" );
        
        $src->close();
    }

    public function testSelectInto() {
        $this->makeTable( "test", "a INT NOT NULL, b INT NOT NULL" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (3, 8)" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (7, 9)" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (11, 11)" );
        
		//-----------------------------------------------------------
		$sink = new gpArraySink();
        $this->gp->select_into( "select a, b from test order by a, b", $sink );

		$data = $sink->getData();
		
        $this->assertEquals(array(array(3, 8), array(7, 9), array(11, 11)), $data );
    }

    public function testUnbufferedSelectInto() {
        $this->makeTable( "test", "a INT NOT NULL, b INT NOT NULL" );
        $this->gp->set_unbuffered(true);
        $this->gp->mysql_query( "INSERT INTO test VALUES (3, 8)" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (7, 9)" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (11, 11)" );
        
		//-----------------------------------------------------------
		$sink = new gpArraySink();
        $this->gp->select_into( "select a, b from test order by a, b", $sink );

		$data = $sink->getData();
		
        $this->assertEquals(array(array(3, 8), array(7, 9), array(11, 11)), $data );
    }

    public function testTempSink() {
		$snk = $this->gp->make_temp_sink( new gpMySQLTable("?", "a", "b") );
		$table = $snk->getTable();
		
		$snk->putRow( array(4,5) );
		$snk->putRow( array(6,7) );
		$snk->close();
		
		$res = $this->gp->mysql_query( "SELECT a, b FROM ".$table->get_name()." ORDER BY a, b");
		
		$this->assertEquals(array('a' => 4, 'b' => 5), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 4,5, got " . var_export($r, true) );
		$this->assertEquals(array('a' => 6, 'b' => 7), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 6,7, got " . var_export($r, true) );
		$this->assertFalse(  $this->gp->mysql_fetch_assoc($res), "expected next row to be false" );
		
		$this->gp->mysql_free_result($res);
		
		$snk->drop();
    }

    public function testAddArcsFromSourceObject() {
        $this->makeTable( "test", "a INT NOT NULL, b INT NOT NULL" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (1, 11)" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (1, 12)" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (11, 111)" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (11, 112)" );
        
		//-----------------------------------------------------------
        $src = $this->gp->make_source( new gpMySQLTable("test", "a", "b") );
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

    public function testAddArcsFromSourceShorthand() {
        $this->makeTable( "test", "a INT NOT NULL, b INT NOT NULL" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (1, 11)" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (1, 12)" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (11, 111)" );
        $this->gp->mysql_query( "INSERT INTO test VALUES (11, 112)" );
        
		//-----------------------------------------------------------
        $src = $this->gp->add_arcs_from( "test a b" );
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
		
		//-----------------------------------------------------------
		$this->gp->clear();
		$stats = $this->gp->capture_stats_map();
		$this->assertEquals( 0, $stats['ArcCount'], "ArcCount" );

		//$this->gp->setDebug(true);
        $src = $this->gp->add_arcs_from( "select a, b from test" );
		$src->close();
		
		$stats = $this->gp->capture_stats_map();
		$this->assertEquals( 4, $stats['ArcCount'], "ArcCount" );
		
		//-----------------------------------------------------------
		$this->gp->clear();

        $src = $this->gp->add_arcs_from( array("test", "a", "b") );
		$src->close();
		
		$this->assertStatsValue( 'ArcCount', 4 );

		//-----------------------------------------------------------
		$this->gp->clear();

        $src = $this->gp->add_arcs_from( new gpMySQLTable("test", "a", "b") );
		$src->close();
		
		$this->assertStatsValue( 'ArcCount', 4 );
    }

    public function testSuccessorsToSinkObject() {
		$this->gp->add_arcs( array(
			array( 1, 11 ),
			array( 1, 12 ),
			array( 11, 111 ),
			array( 11, 112 ),
		) );
        
		//-----------------------------------------------------------
		$snk = $this->gp->make_temp_sink( new gpMySQLTable("?", "n") );
        $src = $this->gp->traverse_successors( 1, 8, $snk );
        $snk->close();
        $table = $snk->getTable();
        
		$res = $this->gp->mysql_query( "SELECT n FROM ".$table->get_name()." ORDER BY n");
		
		$this->assertEquals(array('n' => 1), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 1 got " . var_export($r, true) );
		$this->assertEquals(array('n' => 11), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 11, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 12), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 12, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 111), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 111, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 112), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 112, got " . var_export($r, true) );
		$this->assertFalse(  $this->gp->mysql_fetch_assoc($res), "expected next row to be false" );
		
		$this->gp->mysql_free_result($res);
        
		//-----------------------------------------------------------
		$this->gp->set_max_allowed_packet(6); //force inserter to flush intermittedly
		
		$snk = $this->gp->make_temp_sink( new gpMySQLTable("?", "n") );
        $src = $this->gp->traverse_successors( 1, 8, $snk );
        $snk->close();
        $table = $snk->getTable();
        
		$res = $this->gp->mysql_query( "SELECT n FROM ".$table->get_name()." ORDER BY n");
		
		$this->assertEquals(array('n' => 1), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 1 got " . var_export($r, true) );
		$this->assertEquals(array('n' => 11), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 11, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 12), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 12, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 111), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 111, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 112), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 112, got " . var_export($r, true) );
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
        $snk = $this->gp->traverse_successors_into( 1, 8, "? n" );
        $snk->close();
        $table = $snk->getTable();
        
		$res = $this->gp->mysql_query( "SELECT n FROM ".$table->get_name()." ORDER BY n");
		
		$this->assertEquals(array('n' => 1), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 1 got " . var_export($r, true) );
		$this->assertEquals(array('n' => 11), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 11, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 12), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 12, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 111), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 111, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 112), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 112, got " . var_export($r, true) );
		$this->assertFalse(  $this->gp->mysql_fetch_assoc($res), "expected next row to be false" );
		
		$this->gp->mysql_free_result($res);
		$snk->drop();

		//---------------------------------------------------------
        $snk = $this->gp->traverse_successors_into( 1, 8, array("?", "n") );
        $snk->close();
        $table = $snk->getTable();
        
		$res = $this->gp->mysql_query( "SELECT n FROM ".$table->get_name()." ORDER BY n");
		
		$this->assertEquals(array('n' => 1), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 1 got " . var_export($r, true) );
		$this->assertEquals(array('n' => 11), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 11, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 12), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 12, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 111), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 111, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 112), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 112, got " . var_export($r, true) );
		$this->assertFalse(  $this->gp->mysql_fetch_assoc($res), "expected next row to be false" );
		
		$this->gp->mysql_free_result($res);
		$snk->drop();

		//---------------------------------------------------------
        $snk = $this->gp->traverse_successors_into( 1, 8, new gpMySQLTable("?", "n") );
        $snk->close();
        $table = $snk->getTable();
        
		$res = $this->gp->mysql_query( "SELECT n FROM ".$table->get_name()." ORDER BY n");
		
		$this->assertEquals(array('n' => 1), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 1 got " . var_export($r, true) );
		$this->assertEquals(array('n' => 11), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 11, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 12), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 12, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 111), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 111, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 112), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 112, got " . var_export($r, true) );
		$this->assertFalse(  $this->gp->mysql_fetch_assoc($res), "expected next row to be false" );
		
		$this->gp->mysql_free_result($res);
		$snk->drop();

		//---------------------------------------------------------
        $this->makeTable( "test_n", "n INT NOT NULL" );

        $table = new gpMySQLTable("test_n", "n");
        $snk = $this->gp->traverse_successors_into( 1, 8, $table );
        $snk->close();
        
		$res = $this->gp->mysql_query( "SELECT n FROM ".$table->get_name()." ORDER BY n");
		
		$this->assertEquals(array('n' => 1), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 1 got " . var_export($r, true) );
		$this->assertEquals(array('n' => 11), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 11, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 12), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 12, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 111), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 111, got " . var_export($r, true) );
		$this->assertEquals(array('n' => 112), $r = $this->gp->mysql_fetch_assoc($res) , "expected row to be 112, got " . var_export($r, true) );
		$this->assertFalse(  $this->gp->mysql_fetch_assoc($res), "expected next row to be false" );
		
		$this->gp->mysql_free_result($res);
    }

}

