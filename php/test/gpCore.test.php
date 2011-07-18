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
	
    //TODO: add all the tests we have in the talkback test suit
    
}

