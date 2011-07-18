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

    protected function makeTable( $table, $fieldSpec ) {
		$sql = "CREATE TEMPORARY TABLE IF NOT EXISTS " . $table; 
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
        $p = $this->makeWikiTable( "page", "page_id INT NOT NULL, page_namespace INT NOT NULL, page_title VARCHAR(255) NOT NULL" );
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
        
        $cl = $this->makeWikiTable( "categorylinks", "cl_from INT NOT NULL, cl_to VARCHAR(255) NOT NULL" );
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

    public function testAddArcsFromCategoryStructure() {
        $this->makeWikiStructure();
        
		$this->gp->add_arcs_from_category_structure();

		//-----------------------------------------------------------
		$a = $this->capture_list_successors( 10 );
        $this->assertEquals(array(array(110), array(120)), $a );

		$a = $this->capture_list_predecessors( 1120 );
        $this->assertEquals(array(array(120), array(1110)), $a );

		$a = $this->capture_traverse_successors( 110, 5 );
        $this->assertEquals(array(array(110), array(1110), array(2110), array(1120)), $a );
    }

    public function testWikiSubcategories() {
        $this->makeWikiStructure();
		
		$a = $this->gp->capture_wiki_subcategories("topics", 5);
        $this->assertEquals(array(array("Topics"), array("Beer"), array("Bad_Cheese"), array("Cheese")), $a );
	}

}

