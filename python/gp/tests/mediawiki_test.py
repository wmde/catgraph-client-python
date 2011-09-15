from gp.mediawiki import *
from gp.client import *
from gp.mysql import *
from test_base import *

import unittest

class MediaWikiTest (SlaveTestBase, unittest.TestCase):
	def setUp(self) :
		global $gpTestGraphCorePath
		global $gpTestMySQLHost, $gpTestMySQLUser, $gpTestMySQLPassword, $gpTestMySQLDatabase, $gpTestMediaWikiTablePrefix
		
		self.dump = new gpPipeSink( STDOUT )

		try :
			self.gp = gpMediaWikiGlue::new_slave_connection( $gpTestGraphCorePath )
			self.gp.connect()
		} catch ( gpException $ex ) :
			print("Unable to launch graphcore instance from $gpTestGraphCorePath, please make sure graphcore is installed and check the \$gpTestGraphCorePath configuration options in gpTestConfig.php.\nOriginal error: " . $ex.getMessage() . "\n")
			exit(10)
		

		try :
			self.gp.mysql_connect($gpTestMySQLHost, $gpTestMySQLUser, $gpTestMySQLPassword)
			self.gp.mysql_select_db($gpTestMySQLDatabase)
			self.gp.set_table_prefix($gpTestMediaWikiTablePrefix)
		} catch ( gpException $ex ) :
			print("Unable to connect to table $gpTestMediaWikiDatabase on MySQL host $gpTestMySQLHost as $gpTestMySQLUser, please make sure MySQL is running and check the \$gpTestMySQLHost and related configuration options in gpTestConfig.php.\nOriginal error: " . $ex.getMessage() . "\n")
			exit(10)
		
	

    def _makeTable( self, $table, $fieldSpec, $temp = false ) :
		$t = $temp ? " TEMPORARY " : ""
		$sql = "CREATE $t TABLE IF NOT EXISTS " . $table
		$sql .= "("
		$sql .= $fieldSpec
		$sql .= ")"
		
		self.gp.mysql_query($sql)
		
		$sql = "TRUNCATE TABLE $table"
		self.gp.mysql_query($sql)
	

	def makeWikiTable( self, $name, $spec ) :
		global $gpTestMediaWikiTablePrefix
		$name = "$gpTestMediaWikiTablePrefix$name"
		
        self.makeTable( $name, $spec )
        return $name
    

	def makeWikiStructure( self ) :
        $p = self.makeWikiTable( "page", "page_id INT NOT NULL, page_namespace INT NOT NULL, page_title VARCHAR(255) NOT NULL, PRIMARY KEY (page_id), UNIQUE KEY (page_namespace, page_title)" )
        self.gp.mysql_query( "TRUNCATE $p" )
        
        self.gp.mysql_query( "INSERT INTO $p VALUES (1, ".NS_MAIN.", 'Main_Page')" )
        self.gp.mysql_query( "INSERT INTO $p VALUES (2, ".NS_PROJECT.", 'Help_Out')" )
        
        self.gp.mysql_query( "INSERT INTO $p VALUES (10, ".NS_CATEGORY.", 'ROOT')" )
        self.gp.mysql_query( "INSERT INTO $p VALUES (20, ".NS_CATEGORY.", 'Portals')" )
        self.gp.mysql_query( "INSERT INTO $p VALUES (110, ".NS_CATEGORY.", 'Topics')" )
        self.gp.mysql_query( "INSERT INTO $p VALUES (1110, ".NS_CATEGORY.", 'Beer')" )
        self.gp.mysql_query( "INSERT INTO $p VALUES (1111, ".NS_MAIN.", 'Lager')" )
        self.gp.mysql_query( "INSERT INTO $p VALUES (1112, ".NS_MAIN.", 'Pils')" )
        self.gp.mysql_query( "INSERT INTO $p VALUES (2110, ".NS_CATEGORY.", 'Cheese')" )
        self.gp.mysql_query( "INSERT INTO $p VALUES (120, ".NS_CATEGORY.", 'Maintenance')" )
        self.gp.mysql_query( "INSERT INTO $p VALUES (1120, ".NS_CATEGORY.", 'Bad_Cheese')" )
        self.gp.mysql_query( "INSERT INTO $p VALUES (1122, ".NS_MAIN.", 'Toe_Cheese')" )
        self.gp.mysql_query( "INSERT INTO $p VALUES (333, ".NS_TEMPLATE.", 'Yuck')" )
        
        $cl = self.makeWikiTable( "categorylinks", "cl_from INT NOT NULL, cl_to VARCHAR(255) NOT NULL, PRIMARY KEY (cl_from, cl_to), INDEX cl_to (cl_to)" )
        self.gp.mysql_query( "TRUNCATE $cl" )
        
        self.gp.mysql_query( "INSERT INTO $cl VALUES (1, 'Portals')" )
        self.gp.mysql_query( "INSERT INTO $cl VALUES (2, 'Portals')" )
        self.gp.mysql_query( "INSERT INTO $cl VALUES (20, 'ROOT')" )
        self.gp.mysql_query( "INSERT INTO $cl VALUES (120, 'ROOT')" )
        self.gp.mysql_query( "INSERT INTO $cl VALUES (110, 'ROOT')" )
        self.gp.mysql_query( "INSERT INTO $cl VALUES (1110, 'Topics')" )
        self.gp.mysql_query( "INSERT INTO $cl VALUES (2110, 'Topics')" )
        self.gp.mysql_query( "INSERT INTO $cl VALUES (1111, 'Beer')" )
        self.gp.mysql_query( "INSERT INTO $cl VALUES (1112, 'Beer')" )
        self.gp.mysql_query( "INSERT INTO $cl VALUES (1120, 'Maintenance')" )
        self.gp.mysql_query( "INSERT INTO $cl VALUES (1120, 'Cheese')" )
        self.gp.mysql_query( "INSERT INTO $cl VALUES (1120, 'Cruft')" )
        self.gp.mysql_query( "INSERT INTO $cl VALUES (1122, 'Bad_Cheese')" )

        $tl = self.makeWikiTable( "templatelinks", "tl_from INT NOT NULL, tl_namespace INT NOT NULL, tl_title VARCHAR(255) NOT NULL, PRIMARY KEY (tl_from, tl_namespace, tl_title), INDEX tl_to (tl_namespace, tl_title)" )
        self.gp.mysql_query( "TRUNCATE $tl" )
        
        self.gp.mysql_query( "INSERT INTO $tl VALUES (1122, ".NS_TEMPLATE.", 'Yuck')" )
        self.gp.mysql_query( "INSERT INTO $tl VALUES (1111, ".NS_TEMPLATE.", 'Yuck')" )
	
        
        
	###########################################

    def test_TraverseSuccessors( self ) :
		self.gp.add_arcs( array(
			array( 1, 11 ),
			array( 1, 12 ),
			array( 11, 111 ),
			array( 11, 112 ),
			array( 111, 1111 ),
			array( 111, 1112 ),
			array( 112, 1121 ),
		) )
		
		self.assertStatsValue( 'ArcCount', 7 )
		
		#--------------------------------------------
		$succ = self.gp.capture_traverse_successors( 11, 5 )

		self.assertEquals( array( array(11), array(111), array(112), array(1111), array(1112), array(1121), ), $succ )
	
        
	###########################################

    def test_AddArcsFromCategoryStructure( self ) :
        self.makeWikiStructure()
        
		#-----------------------------------------------------------
		self.gp.add_arcs_from_category_structure()

		#-----------------------------------------------------------
		$a = self.gp.capture_list_successors( 10 )
        self.assertEquals(array(array(20), array(110), array(120)), $a )

		$a = self.gp.capture_list_predecessors( 1120 )
        self.assertEquals(array(array(120), array(2110)), $a )

		$a = self.gp.capture_traverse_successors( 110, 5 )
        self.assertEquals(array(array(110), array(1110), array(2110), array(1120)), $a )
    

    def test_GetSubcategories( self ) :
        self.makeWikiStructure()
		self.gp.add_arcs_from_category_structure()

		#-----------------------------------------------------------
		$a = self.gp.get_subcategories("topics", 5)
        self.assertEquals(array(array("Topics"), 
									array("Beer"), 
									array("Bad_Cheese"), 
									array("Cheese")), $a )

		#-----------------------------------------------------------
		$a = self.gp.get_subcategories("topics", 5, "maintenance")
        self.assertEquals(array(array("Topics"), 
									array("Beer"), 
									array("Cheese")), $a )
	

	###########################################
    def test_AddSubcategories( self ) :
        self.makeWikiStructure()
		self.gp.add_arcs_from_category_structure()

		$set = new gpPageSet(self.gp)
		$set.create_table()
		
		#-----------------------------------------------------------
		$set.clear()
		$ok = $set.add_subcategories("topics", 5)
		self.assertTrue( $ok )
		
		$a = $set.capture()
        self.assertEquals(array(array(110, NS_CATEGORY, "Topics"), 
									array(1110, NS_CATEGORY, "Beer"), 
									array(1120, NS_CATEGORY, "Bad_Cheese"), 
									array(2110, NS_CATEGORY, "Cheese")), $a )
		
		#-----------------------------------------------------------
		$set.clear()
		$ok = $set.add_subcategories("Portals", 5)
		self.assertTrue( $ok )
		
		$a = $set.capture()
        self.assertEquals(array(array(20, NS_CATEGORY, "Portals")), $a )

        #-----------------------------------------------------------
        $set.dispose()
	
	
    def test_AddPagesTranscluding( self ) :
        self.makeWikiStructure()
		self.gp.add_arcs_from_category_structure()

		$set = new gpPageSet(self.gp)
		$set.create_table()
		
		#-----------------------------------------------------------
		$set.clear()
		$ok = $set.add_pages_transclusing("yuck")
		self.assertTrue( $ok )
		
		$a = $set.capture()
        self.assertEquals(array(array(1111, NS_MAIN, "Lager"), 
									array(1122, NS_MAIN, "Toe_Cheese")), $a )
		
        #-----------------------------------------------------------
        $set.dispose()
	
	
    def test_AddPagesIn( self ) :
        self.makeWikiStructure()
		self.gp.add_arcs_from_category_structure()

		$set = new gpPageSet(self.gp)
		$set.create_table()
		
		#-----------------------------------------------------------
		$set.clear()
		$ok = $set.add_pages_in("topics", null, 5)
		self.assertTrue( $ok )
		
		$a = $set.capture()
		$expected = array(array(110, NS_CATEGORY, "Topics"), 
									array(1110, NS_CATEGORY, "Beer"), 
									array(1111, NS_MAIN, "Lager"), 
									array(1112, NS_MAIN, "Pils"), 
									array(1120, NS_CATEGORY, "Bad_Cheese"), 
									array(1122, NS_MAIN, "Toe_Cheese"), 
									array(2110, NS_CATEGORY, "Cheese"))
		
        self.assertEquals($expected, $a )

		#-----------------------------------------------------------
		$set.clear()
		$ok = $set.add_pages_in("topics", null, 5)
		self.assertTrue( $ok )
		
		$a = $set.capture( NS_MAIN )
        self.assertEquals(array(array(1111, NS_MAIN, "Lager"), 
									array(1112, NS_MAIN, "Pils"), 
									array(1122, NS_MAIN, "Toe_Cheese")), $a )

		#-----------------------------------------------------------
		$set.clear()
		$ok = $set.add_pages_in("Portals", NS_MAIN, 5)
		self.assertTrue( $ok )
		
		$a = $set.capture()
        self.assertEquals(array(array(1, NS_MAIN, "Main_Page"),
									array(20, NS_CATEGORY, "Portals")), $a )

		#-----------------------------------------------------------
		$set.clear()
		$ok = $set.add_pages_in("portals", array(NS_MAIN, NS_PROJECT), 5)
		self.assertTrue( $ok )
		
		$a = $set.capture( array(NS_MAIN, NS_PROJECT) )
        self.assertEquals(array(array(1, NS_MAIN, "Main_Page"), 
									array(2, NS_PROJECT, "Help_Out")), $a )

        #-----------------------------------------------------------
        $set.dispose()
	

    def test_BufferedAddPagesIn( self ) :
        self.makeWikiStructure()
		self.gp.add_arcs_from_category_structure()

		$set = new gpPageSet(self.gp)
		$set.set_expect_big(false)
		$set.create_table()
		
		#-----------------------------------------------------------
		$set.clear()
		$ok = $set.add_pages_in("topics", null, 5)
		self.assertTrue( $ok )
		
		$a = $set.capture()
		$expected = array(array(110, NS_CATEGORY, "Topics"), 
									array(1110, NS_CATEGORY, "Beer"), 
									array(1111, NS_MAIN, "Lager"), 
									array(1112, NS_MAIN, "Pils"), 
									array(1120, NS_CATEGORY, "Bad_Cheese"), 
									array(1122, NS_MAIN, "Toe_Cheese"), 
									array(2110, NS_CATEGORY, "Cheese"))
		
        self.assertEquals($expected, $a )

        #-----------------------------------------------------------
        $set.dispose()
	

    def test_SubtractPageSet( self ) :
        self.makeWikiStructure()
		self.gp.add_arcs_from_category_structure()

		$set = new gpPageSet(self.gp)
		$set.create_table()
		
		$rset = new gpPageSet(self.gp)
		$rset.create_table()
		
		#-----------------------------------------------------------
		$ok = $set.add_pages_in("topics", null, 5)
		$ok = $rset.add_pages_in("Maintenance", null, 5)

		$ok = $set.subtract_page_set( $rset )
		self.assertTrue( $ok )
		
		$a = $set.capture()
		$expected = array(array(110, NS_CATEGORY, "Topics"), 
									array(1110, NS_CATEGORY, "Beer"), 
									array(1111, NS_MAIN, "Lager"), 
									array(1112, NS_MAIN, "Pils"), 
									array(2110, NS_CATEGORY, "Cheese"))
		
        self.assertEquals($expected, $a )
        
        #-----------------------------------------------------------
        $set.dispose()
        $rset.dispose()
	

    def test_RetainPageSet( self ) :
        self.makeWikiStructure()
		self.gp.add_arcs_from_category_structure()

		$set = new gpPageSet(self.gp)
		$set.create_table()
		
		$rset = new gpPageSet(self.gp)
		$rset.create_table()
		
		#-----------------------------------------------------------
		$ok = $set.add_pages_in("topics", null, 5)
		$ok = $rset.add_pages_in("Maintenance", null, 5)

		$ok = $set.retain_page_set( $rset )
		self.assertTrue( $ok )
		
		$a = $set.capture()
		$expected = array(array(1120, NS_CATEGORY, "Bad_Cheese"), 
							array(1122, NS_MAIN, "Toe_Cheese"))
		
        self.assertEquals($expected, $a )
        
        #-----------------------------------------------------------
        $set.dispose()
        $rset.dispose()
	

    def test_AddPageSet( self ) :
        self.makeWikiStructure()
		self.gp.add_arcs_from_category_structure()

		$beer = new gpPageSet(self.gp)
		$beer.create_table()
		
		$cheese = new gpPageSet(self.gp)
		$cheese.create_table()
		
		#-----------------------------------------------------------
		$ok = $cheese.add_pages_in("Cheese", null, 5)
		$ok = $beer.add_pages_in("Beer", null, 5)

		$ok = $cheese.add_page_set( $beer )
		self.assertTrue( $ok )
		
		$a = $cheese.capture()
		$expected = array(array(1110, NS_CATEGORY, "Beer"), 
							array(1111, NS_MAIN, "Lager"), 
							array(1112, NS_MAIN, "Pils"), 
							array(1120, NS_CATEGORY, "Bad_Cheese"), 
							array(1122, NS_MAIN, "Toe_Cheese"),
							array(2110, NS_CATEGORY, "Cheese")       )
		
        self.assertEquals($expected, $a )
        
        #-----------------------------------------------------------
        $beer.dispose()
        $cheese.dispose()
	

    def test_DeleteWhere( self ) :
        self.makeWikiStructure()
		self.gp.add_arcs_from_category_structure()

		$set = new gpPageSet(self.gp)
		$set.create_table()
		
		$set.add_pages_in("topics", null, 5)
		
		#-----------------------------------------------------------
		$set.delete_where( "where page_namespace = " . NS_CATEGORY )
		
		$a = $set.capture()
		$expected = array(array(1111, NS_MAIN, "Lager"), 
							array(1112, NS_MAIN, "Pils"), 
							array(1122, NS_MAIN, "Toe_Cheese"))
		
        self.assertEquals($expected, $a )
        
 		#-----------------------------------------------------------
       $set.dispose()
	

    def test_DeleteUsing( self ) :
        self.makeWikiStructure()
		self.gp.add_arcs_from_category_structure()

		$set = new gpPageSet(self.gp)
		$set.create_table()
		
		$set.add_pages_in("topics", null, 5)
		
		#-----------------------------------------------------------
		$sql = " JOIN " . self.gp.wiki_table("templatelinks") . " as X "
		$sql .= " ON T.page_id = X.tl_from "
		$sql .= " WHERE X.tl_namespace = " . NS_TEMPLATE
		$sql .= " AND X.tl_title = " . self.gp.quote_string("Yuck")
		
		$set.delete_using( $sql )
		
		$a = $set.capture(NS_MAIN)
		$expected = array(array(1112, NS_MAIN, "Pils"))
		
        self.assertEquals($expected, $a )
        
		#-----------------------------------------------------------
        $set.dispose()
	

    def test_StripNamespace( self ) :
        self.makeWikiStructure()
		self.gp.add_arcs_from_category_structure()

		$set = new gpPageSet(self.gp)
		$set.create_table()
		
		#-----------------------------------------------------------
		$set.clear()
		$set.add_pages_in("topics", null, 5)
		$set.strip_namespace( NS_CATEGORY )
		
		$a = $set.capture()
		$expected = array(array(1111, NS_MAIN, "Lager"), 
							array(1112, NS_MAIN, "Pils"), 
							array(1122, NS_MAIN, "Toe_Cheese"))
		
        self.assertEquals($expected, $a )
		
		#-----------------------------------------------------------
		$set.clear()
		$set.add_pages_in("Portals", null, 5)
		$set.strip_namespace( array(NS_CATEGORY, NS_PROJECT) )
		
		$a = $set.capture()
		$expected = array(array(1, NS_MAIN, "Main_Page"))
		
        self.assertEquals($expected, $a )
        
  		#-----------------------------------------------------------
		$set.dispose()
	

    def test_RetainNamespace( self ) :
        self.makeWikiStructure()
		self.gp.add_arcs_from_category_structure()

		$set = new gpPageSet(self.gp)
		$set.create_table()
		
		#-----------------------------------------------------------
		$set.clear()
		$set.add_pages_in("topics", null, 5)
		$set.retain_namespace( array(NS_MAIN) )
		
		$a = $set.capture()
		$expected = array(array(1111, NS_MAIN, "Lager"), 
							array(1112, NS_MAIN, "Pils"), 
							array(1122, NS_MAIN, "Toe_Cheese"))
		
        self.assertEquals($expected, $a )
		
		#-----------------------------------------------------------
		$set.clear()
		$set.add_pages_in("Portals", null, 5)
		$set.retain_namespace( NS_MAIN )
		
		$a = $set.capture()
		$expected = array(array(1, NS_MAIN, "Main_Page"))
		
        self.assertEquals($expected, $a )
	



