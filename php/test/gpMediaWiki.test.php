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
        $this->gp->mysql_query( "TRUNCATE $p" );
        
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
        $this->gp->mysql_query( "INSERT INTO $p VALUES (333, ".NS_TEMPLATE.", 'Yuck')" );
        
        $cl = $this->makeWikiTable( "categorylinks", "cl_from INT NOT NULL, cl_to VARCHAR(255) NOT NULL, PRIMARY KEY (cl_from, cl_to), INDEX cl_to (cl_to)" );
        $this->gp->mysql_query( "TRUNCATE $cl" );
        
        $this->gp->mysql_query( "INSERT INTO $cl VALUES (1, 'Portals')" );
        $this->gp->mysql_query( "INSERT INTO $cl VALUES (2, 'Portals')" );
        $this->gp->mysql_query( "INSERT INTO $cl VALUES (20, 'ROOT')" );
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

        $tl = $this->makeWikiTable( "templatelinks", "tl_from INT NOT NULL, tl_namespace INT NOT NULL, tl_title VARCHAR(255) NOT NULL, PRIMARY KEY (tl_from, tl_namespace, tl_title), INDEX tl_to (tl_namespace, tl_title)" );
        $this->gp->mysql_query( "TRUNCATE $tl" );
        
        $this->gp->mysql_query( "INSERT INTO $tl VALUES (1122, ".NS_TEMPLATE.", 'Yuck')" );
        $this->gp->mysql_query( "INSERT INTO $tl VALUES (1111, ".NS_TEMPLATE.", 'Yuck')" );
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
        $this->assertEquals(array(array(20), array(110), array(120)), $a );

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

    public function testAddSubcategories() {
        $this->makeWikiStructure();
		$this->gp->add_arcs_from_category_structure();

		$set = new gpPageSet($this->gp);
		$set->create_table();
		
		//-----------------------------------------------------------
		$set->clear();
		$ok = $set->add_subcategories("topics", 5);
		$this->assertTrue( $ok );
		
		$a = $set->capture();
        $this->assertEquals(array(array(110, NS_CATEGORY, "Topics"), 
									array(1110, NS_CATEGORY, "Beer"), 
									array(1120, NS_CATEGORY, "Bad_Cheese"), 
									array(2110, NS_CATEGORY, "Cheese")), $a );
		
		//-----------------------------------------------------------
		$set->clear();
		$ok = $set->add_subcategories("Portals", 5);
		$this->assertTrue( $ok );
		
		$a = $set->capture();
        $this->assertEquals(array(array(20, NS_CATEGORY, "Portals")), $a );

        //-----------------------------------------------------------
        $set->dispose();
	}
	
    public function testAddPagesTranscluding() {
        $this->makeWikiStructure();
		$this->gp->add_arcs_from_category_structure();

		$set = new gpPageSet($this->gp);
		$set->create_table();
		
		//-----------------------------------------------------------
		$set->clear();
		$ok = $set->add_pages_transclusing("yuck");
		$this->assertTrue( $ok );
		
		$a = $set->capture();
        $this->assertEquals(array(array(1111, NS_MAIN, "Lager"), 
									array(1122, NS_MAIN, "Toe_Cheese")), $a );
		
        //-----------------------------------------------------------
        $set->dispose();
	}
	
    public function testAddPagesIn() {
        $this->makeWikiStructure();
		$this->gp->add_arcs_from_category_structure();

		$set = new gpPageSet($this->gp);
		$set->create_table();
		
		//-----------------------------------------------------------
		$set->clear();
		$ok = $set->add_pages_in("topics", null, 5);
		$this->assertTrue( $ok );
		
		$a = $set->capture();
		$expected = array(array(110, NS_CATEGORY, "Topics"), 
									array(1110, NS_CATEGORY, "Beer"), 
									array(1111, NS_MAIN, "Lager"), 
									array(1112, NS_MAIN, "Pils"), 
									array(1120, NS_CATEGORY, "Bad_Cheese"), 
									array(1122, NS_MAIN, "Toe_Cheese"), 
									array(2110, NS_CATEGORY, "Cheese"));
		
        $this->assertEquals($expected, $a );

		//-----------------------------------------------------------
		$set->clear();
		$ok = $set->add_pages_in("topics", null, 5);
		$this->assertTrue( $ok );
		
		$a = $set->capture( NS_MAIN );
        $this->assertEquals(array(array(1111, NS_MAIN, "Lager"), 
									array(1112, NS_MAIN, "Pils"), 
									array(1122, NS_MAIN, "Toe_Cheese")), $a );

		//-----------------------------------------------------------
		$set->clear();
		$ok = $set->add_pages_in("Portals", NS_MAIN, 5);
		$this->assertTrue( $ok );
		
		$a = $set->capture();
        $this->assertEquals(array(array(1, NS_MAIN, "Main_Page"),
									array(20, NS_CATEGORY, "Portals")), $a );

		//-----------------------------------------------------------
		$set->clear();
		$ok = $set->add_pages_in("portals", array(NS_MAIN, NS_PROJECT), 5);
		$this->assertTrue( $ok );
		
		$a = $set->capture( array(NS_MAIN, NS_PROJECT) );
        $this->assertEquals(array(array(1, NS_MAIN, "Main_Page"), 
									array(2, NS_PROJECT, "Help_Out")), $a );

        //-----------------------------------------------------------
        $set->dispose();
	}

    public function testSubtractPageSet() {
        $this->makeWikiStructure();
		$this->gp->add_arcs_from_category_structure();

		$set = new gpPageSet($this->gp);
		$set->create_table();
		
		$rset = new gpPageSet($this->gp);
		$rset->create_table();
		
		//-----------------------------------------------------------
		$ok = $set->add_pages_in("topics", null, 5);
		$ok = $rset->add_pages_in("Maintenance", null, 5);

		$ok = $set->subtract_page_set( $rset );
		$this->assertTrue( $ok );
		
		$a = $set->capture();
		$expected = array(array(110, NS_CATEGORY, "Topics"), 
									array(1110, NS_CATEGORY, "Beer"), 
									array(1111, NS_MAIN, "Lager"), 
									array(1112, NS_MAIN, "Pils"), 
									array(2110, NS_CATEGORY, "Cheese"));
		
        $this->assertEquals($expected, $a );
        
        //-----------------------------------------------------------
        $set->dispose();
        $rset->dispose();
	}

    public function testRetainPageSet() {
        $this->makeWikiStructure();
		$this->gp->add_arcs_from_category_structure();

		$set = new gpPageSet($this->gp);
		$set->create_table();
		
		$rset = new gpPageSet($this->gp);
		$rset->create_table();
		
		//-----------------------------------------------------------
		$ok = $set->add_pages_in("topics", null, 5);
		$ok = $rset->add_pages_in("Maintenance", null, 5);

		$ok = $set->retain_page_set( $rset );
		$this->assertTrue( $ok );
		
		$a = $set->capture();
		$expected = array(array(1120, NS_CATEGORY, "Bad_Cheese"), 
							array(1122, NS_MAIN, "Toe_Cheese"));
		
        $this->assertEquals($expected, $a );
        
        //-----------------------------------------------------------
        $set->dispose();
        $rset->dispose();
	}

    public function testAddPageSet() {
        $this->makeWikiStructure();
		$this->gp->add_arcs_from_category_structure();

		$beer = new gpPageSet($this->gp);
		$beer->create_table();
		
		$cheese = new gpPageSet($this->gp);
		$cheese->create_table();
		
		//-----------------------------------------------------------
		$ok = $cheese->add_pages_in("Cheese", null, 5);
		$ok = $beer->add_pages_in("Beer", null, 5);

		$ok = $cheese->add_page_set( $beer );
		$this->assertTrue( $ok );
		
		$a = $cheese->capture();
		$expected = array(array(1110, NS_CATEGORY, "Beer"), 
							array(1111, NS_MAIN, "Lager"), 
							array(1112, NS_MAIN, "Pils"), 
							array(1120, NS_CATEGORY, "Bad_Cheese"), 
							array(1122, NS_MAIN, "Toe_Cheese"),
							array(2110, NS_CATEGORY, "Cheese")       );
		
        $this->assertEquals($expected, $a );
        
        //-----------------------------------------------------------
        $beer->dispose();
        $cheese->dispose();
	}

    public function testDeleteWhere() {
        $this->makeWikiStructure();
		$this->gp->add_arcs_from_category_structure();

		$set = new gpPageSet($this->gp);
		$set->create_table();
		
		$set->add_pages_in("topics", null, 5);
		
		//-----------------------------------------------------------
		$set->delete_where( "where page_namespace = " . NS_CATEGORY );
		
		$a = $set->capture();
		$expected = array(array(1111, NS_MAIN, "Lager"), 
							array(1112, NS_MAIN, "Pils"), 
							array(1122, NS_MAIN, "Toe_Cheese"));
		
        $this->assertEquals($expected, $a );
        
 		//-----------------------------------------------------------
       $set->dispose();
	}

    public function testDeleteUsing() {
        $this->makeWikiStructure();
		$this->gp->add_arcs_from_category_structure();

		$set = new gpPageSet($this->gp);
		$set->create_table();
		
		$set->add_pages_in("topics", null, 5);
		
		//-----------------------------------------------------------
		$sql = " JOIN " . $this->gp->wiki_table("templatelinks") . " as X ";
		$sql .= " ON T.page_id = X.tl_from ";
		$sql .= " WHERE X.tl_namespace = " . NS_TEMPLATE;
		$sql .= " AND X.tl_title = " . $this->gp->quote_string("Yuck");
		
		$set->delete_using( $sql );
		
		$a = $set->capture(NS_MAIN);
		$expected = array(array(1112, NS_MAIN, "Pils"));
		
        $this->assertEquals($expected, $a );
        
		//-----------------------------------------------------------
        $set->dispose();
	}

    public function testStripNamespace() {
        $this->makeWikiStructure();
		$this->gp->add_arcs_from_category_structure();

		$set = new gpPageSet($this->gp);
		$set->create_table();
		
		//-----------------------------------------------------------
		$set->clear();
		$set->add_pages_in("topics", null, 5);
		$set->strip_namespace( NS_CATEGORY );
		
		$a = $set->capture();
		$expected = array(array(1111, NS_MAIN, "Lager"), 
							array(1112, NS_MAIN, "Pils"), 
							array(1122, NS_MAIN, "Toe_Cheese"));
		
        $this->assertEquals($expected, $a );
		
		//-----------------------------------------------------------
		$set->clear();
		$set->add_pages_in("Portals", null, 5);
		$set->strip_namespace( array(NS_CATEGORY, NS_PROJECT) );
		
		$a = $set->capture();
		$expected = array(array(1, NS_MAIN, "Main_Page"));
		
        $this->assertEquals($expected, $a );
        
  		//-----------------------------------------------------------
		$set->dispose();
	}

    public function testRetainNamespace() {
        $this->makeWikiStructure();
		$this->gp->add_arcs_from_category_structure();

		$set = new gpPageSet($this->gp);
		$set->create_table();
		
		//-----------------------------------------------------------
		$set->clear();
		$set->add_pages_in("topics", null, 5);
		$set->retain_namespace( array(NS_MAIN) );
		
		$a = $set->capture();
		$expected = array(array(1111, NS_MAIN, "Lager"), 
							array(1112, NS_MAIN, "Pils"), 
							array(1122, NS_MAIN, "Toe_Cheese"));
		
        $this->assertEquals($expected, $a );
		
		//-----------------------------------------------------------
		$set->clear();
		$set->add_pages_in("Portals", null, 5);
		$set->retain_namespace( NS_MAIN );
		
		$a = $set->capture();
		$expected = array(array(1, NS_MAIN, "Main_Page"));
		
        $this->assertEquals($expected, $a );
	}

}

