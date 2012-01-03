<?php
require_once('gpTestBase.php');
 
/**
 * Tests core functions via clinet lib
 */
class gpCoreTest extends gpSlaveTestBase
{
    
    //// Core Functions ///////////////////////////////////////////////////////////////
    
    public function testAddArcs() {
		$this->gp->add_arcs( array(
			array( 1, 11 ),
			array( 1, 12 ),
			array( 11, 111 ),
			array( 11, 112 ),
		) );
		
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

		// ------------------------------------------------------
		
		$this->gp->add_arcs( array(
			array( 1, 11 ),
			array( 11, 112 ),
			array( 2, 21 ),
		) );

		$this->assertStatsValue( 'ArcCount', 5 );

		$arcs = $this->gp->capture_list_successors( 2 );
		$this->assertTrue( gpConnectionTestBase::setEquals( $arcs, array(
			array( 21 ),
		) ), "sucessors of (2)" );
		
    }
    
    public function testClear() {
		$this->gp->add_arcs( array(
			array( 1, 11 ),
			array( 1, 12 ),
			array( 11, 111 ),
			array( 11, 112 ),
		) );
		
		$this->assertStatsValue( 'ArcCount', 4 );
		
		$this->gp->clear();
		
		$arcs = $this->gp->capture_list_successors( 1 );
		
		$this->assertEmpty( $arcs );
		$this->assertStatsValue( 'ArcCount', 0 );

		//--------------------------------------------
		$this->gp->add_arcs( array(
			array( 1, 11 ),
			array( 1, 12 ),
			array( 11, 111 ),
			array( 11, 112 ),
		) );
		
		$this->assertStatsValue( 'ArcCount', 4 );
	}

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
	
    public function testTraverseSuccessorsWithout() {
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
		$succ = $this->gp->capture_traverse_successors_without( 11, 5, 111, 5 );

		$this->assertEquals( array( array(11), array(112), array(1121), ), $succ );
	}

    public function testSetMeta() { #TODO: port to python
		//define var
		$this->gp->set_meta("foo", 1234);
		$val = $this->gp->get_meta_value("foo");
		$this->assertEquals( "1234", $val );
		
		//redefine var
		$this->gp->set_meta("foo", "bla/bla");
		$val = $this->gp->get_meta_value("foo");
		$this->assertEquals( "bla/bla", $val );
		
		# test bad -----------------------------------------
		try {
			$this->gp->set_meta("...", 1234);
			$this->fail( "exception expected" );
		} catch ( gpException $ex ) {
			// ok
		}

		try {
			$this->gp->set_meta("x y", 1234);
			$this->fail( "exception expected" );
		} catch ( gpException $ex ) {
			// ok
		}

		try {
			$this->gp->_set_meta("  ", 1234);
			$this->fail( "exception expected" );
		} catch ( gpException $ex ) {
			// ok
		}

		try {
			$this->gp->set_meta("foo", "bla bla");
			$this->fail( "exception expected" );
		} catch ( gpException $ex ) {
			// ok
		}

		try {
			$this->gp->set_meta("foo", "2<3");
			$this->fail( "exception expected" );
		} catch ( gpException $ex ) {
			// ok
		}
	}

    public function testGetMeta() { #TODO: port to python
		//get undefined
		$val = $this->gp->try_get_meta_value("foo");
		$this->assertEquals( false, $val );
		
		//set var, and get value
		$this->gp->set_meta("foo", "xxx");
		$val = $this->gp->get_meta_value("foo");
		$this->assertEquals( "xxx", $val );
		
		//remove var, then get value
		$this->gp->remove_meta("foo");
		$val = $this->gp->try_get_meta_value("foo");
		$this->assertEquals( false, $val );
	}

    public function testRemoveMeta() { #TODO: port to python
		//remove undefined
		$ok = $this->gp->try_remove_meta("foo");
		$this->assertEquals( false, $ok );
		
		//set var, then remove it
		$this->gp->set_meta("foo", "xxx");
		$ok = $this->gp->try_remove_meta("foo");
		$this->assertEquals( "OK", $ok );
	}

    public function testListMeta() { #TODO: port to python
		// assert empty
		$meta = $this->gp->capture_list_meta();
		$this->assertEmpty( $meta );
		
		// add one, assert list
		$this->gp->set_meta("foo", 1234);
		$meta = $this->gp->capture_list_meta_map();
		$this->assertEquals( array("foo" => "1234"), $meta );
		
		// remove one, assert empty
		$this->gp->remove_meta("foo");
		$meta = $this->gp->capture_list_meta();
		$this->assertEmpty( $meta );
	}

    //TODO: add all the tests we have in the talkback test suit
    
}

