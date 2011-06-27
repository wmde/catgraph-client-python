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
    
    //TODO: add all the tests we have in the talkback test suit
    
}

