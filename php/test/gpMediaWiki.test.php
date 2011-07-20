<?php

require_once('gpTestBase.php');
require_once('../gpMediaWiki.php');

error_reporting(E_ALL);

class gpMediaWikiTest extends gpSlaveTestBase {
	public function setUp() {
		global $gpTestGraphCorePath;
		global $gpTestMySQLHost, $gpTestMySQLUser, $gpTestMySQLPassword, $gpTestMySQLDatabase, $gpTestMediaWikiTablePrefix;
		
		$this->dump = new gpPipeSink( STDOUT ); 

		try {
			$this->gp = gpMediaWikiGlue::new_slave_connection( $gpTestGraphCorePath );
			$this->gp->connect();
		} catch ( gpException $ex ) {
			print("Unable to launch graphcore instance from $gpTestGraphCorePath, please make sure graphcore is installed and check the \$gpTestGraphCorePath configuration options in gpTestConfig.php.\nOriginal error: " . $ex->getMessage() . "\n");
			exit(10);
		}

		try {
			$this->gp->mysql_connect($gpTestMySQLHost, $gpTestMySQLUser, $gpTestMySQLPassword);
			$this->gp->mysql_select_db($gpTestMySQLDatabase);
			$this->gp->set_table_prefix($gpTestMediaWikiTablePrefix);
		} catch ( gpException $ex ) {
			print("Unable to connect to table $gpTestMediaWikiDatabase on MySQL host $gpTestMySQLHost as $gpTestMySQLUser, please make sure MySQL is running and check the \$gpTestMySQLHost and related configuration options in gpTestConfig.php.\nOriginal error: " . $ex->getMessage() . "\n");
			exit(10);
		}
	}

    protected function makeTable( $table, $fieldSpec, $temp = false ) {
		$t = $temp ? " TEMPORARY " : "";
		$sql = "CREATE $t TABLE IF NOT EXISTS " . $table; 
		$sql .= "(";
		$sql .= $fieldSpec;
		$sql .= ")";
		
		$this->gp->mysql_query($sql);
		
		$sql = "TRUNCATE TABLE $table";
		$this->gp->mysql_query($sql);
	}

	public function makeWikiTable( $name, $spec ) {
		global $gpTestMediaWikiTablePrefix;
		$name = "$gpTestMediaWikiTablePrefix$name";
		
        $this->makeTable( $name, $spec );
        return $name;
    }

	public function makeWikiStructure( ) {
        $p = $this->makeWikiTable( "page", "page_id INT NOT NULL, page_namespace INT NOT NULL, page_title VARCHAR(255) NOT NULL, PRIMARY KEY (page_id), UNIQUE KEY (page_namespace, page_title)" );
        $this->gp->mysql_query( "INSERT INTO $p VALUES (1, ".NS_MAIN.", 'Main_Page')" );
        $this->gp->mysql_query( "INSERT INTO $p VALUES (2, ".NS_PROJECT.", 'Help_Out')" );
        
        $this->gp->mysql_query( "INSERT INTO $p VALUES (10, ".NS_CATEGORY.", 'ROOT')" );
        $this->gp->mysql_query( "INSERT INTO $p VALUES (20, ".NS_CATEGORY.", 'Portals')" );
        $this->gp->mysql_query( "INSERT INTO $p VALUES (110, ".NS_CATEGORY.", 'Topics')" );
        $this->gp->mysql_query( "INSERT INTO $p VALUES (1110, ".NS_CATEGORY.", 'Beer')" );
        $this->gp->mysql_query( "INSERT INTO $p VALUES (1111, ".NS_MAIN.", 'Lager')" );
        $this->gp->mysql_query( "INSERT INTO $p VALUES (1112, ".NS_MAIN.", 'Pils')" );
        $this->gp->mysql_query( "INSERT INTO $p VALUES (2110, ".NS_CATEGORY.", 'Cheese')" );
        $this->gp->mysql_query( "INSERT INTO $p VALUES (120, ".NS_CATEGORY.", 'Maintenance')" );
        $this->gp->mysql_query( "INSERT INTO $p VALUES (1120, ".NS_CATEGORY.", 'Bad_Cheese')" );
        $this->gp->mysql_query( "INSERT INTO $p VALUES (1122, ".NS_MAIN.", 'Toe_Cheese')" );
        
        $cl = $this->makeWikiTable( "categorylinks", "cl_from INT NOT NULL, cl_to VARCHAR(255) NOT NULL, PRIMARY KEY (cl_from, cl_to), INDEX cl_to (cl_to)" );
        $this->gp->mysql_query( "INSERT INTO $cl VALUES (1, 'Portals')" );
        $this->gp->mysql_query( "INSERT INTO $cl VALUES (2, 'Portals')" );
        $this->gp->mysql_query( "INSERT INTO $cl VALUES (120, 'ROOT')" );
        $this->gp->mysql_query( "INSERT INTO $cl VALUES (110, 'ROOT')" );
        $this->gp->mysql_query( "INSERT INTO $cl VALUES (1110, 'Topics')" );
        $this->gp->mysql_query( "INSERT INTO $cl VALUES (2110, 'Topics')" );
        $this->gp->mysql_query( "INSERT INTO $cl VALUES (1111, 'Beer')" );
        $this->gp->mysql_query( "INSERT INTO $cl VALUES (1112, 'Beer')" );
        $this->gp->mysql_query( "INSERT INTO $cl VALUES (1120, 'Maintenance')" );
        $this->gp->mysql_query( "INSERT INTO $cl VALUES (1120, 'Cheese')" );
        $this->gp->mysql_query( "INSERT INTO $cl VALUES (1120, 'Cruft')" );
        $this->gp->mysql_query( "INSERT INTO $cl VALUES (1122, 'Bad_Cheese')" );
	}
        
        
	//////////////////////////////////////////////////////////////////////////////////////

    public function testTraverseSuccessors() {
		$this->gp->add_arcs( array(
			array( 1, 11 ),
			array( 1, 12 ),
			array( 11, 111 ),
			array( 11, 112 ),
			array( 111, 1111 ),
			array( 111, 1112 ),
			array( 112, 1121 ),
		) );
		
		$this->assertStatsValue( 'ArcCount', 7 );
		
		//--------------------------------------------
		$succ = $this->gp->capture_traverse_successors( 11, 5 );

		$this->assertEquals( array( array(11), array(111), array(112), array(1111), array(1112), array(1121), ), $succ );
	}
        
	//////////////////////////////////////////////////////////////////////////////////////

    public function testAddArcsFromCategoryStructure() {
        $this->makeWikiStructure();
        
		//-----------------------------------------------------------
		$this->gp->add_arcs_from_category_structure();

		//-----------------------------------------------------------
		$a = $this->gp->capture_list_successors( 10 );
        $this->assertEquals(array(array(110), array(120)), $a );

		$a = $this->gp->capture_list_predecessors( 1120 );
        $this->assertEquals(array(array(120), array(2110)), $a );

		$a = $this->gp->capture_traverse_successors( 110, 5 );
        $this->assertEquals(array(array(110), array(1110), array(2110), array(1120)), $a );
    }

    public function testWikiSubcategories() {
        $this->makeWikiStructure();
		$this->gp->add_arcs_from_category_structure();

		//-----------------------------------------------------------
		$a = $this->gp->capture_wiki_subcategories("topics", 5);
        $this->assertEquals(array(array("Topics"), 
									array("Beer"), 
									array("Bad_Cheese"), 
									array("Cheese")), $a );
	}

	/*
    public function testWikiPagesIn() {
        $this->makeWikiStructure();
		$this->gp->add_arcs_from_category_structure();

		//-----------------------------------------------------------
		$a = $this->gp->capture_wiki_pages_in("topics", null, 5);
        $this->assertEquals(array(array("Beer"), 
									array("Lager"), 
									array("Pils"), 
									array("Bad_Cheese"), 
									array("Toe_Cheese"), 
									array("Cheese")), $a );

		//-----------------------------------------------------------
		$a = $this->gp->capture_wiki_pages_in("topics", 0, 5);
        $this->assertEquals(array(array("Lager"), 
									array("Pils"), 
									array("Toe_Cheese")), $a );

		//-----------------------------------------------------------
		$a = $this->gp->capture_wiki_pages_in("portals", array(NS_MAIN, NS_PROJECT), 5);
        $this->assertEquals(array(array("Main_Page"), 
									array("Help_Out")), $a );
	}
	* */

}

