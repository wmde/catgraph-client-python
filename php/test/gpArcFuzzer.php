<?php
require_once('gpFuzzerBase.php');

/**
 * Tests the TCP client connection
 */
class gpArcFuzzer extends gpFuzzerBase 
{
		
		var $offset = 1;
		
		function prepare() {
			global $gpTestGraphName;

			//in case we use a persistent graph, fund an unused offset
			while ( $this->offset < 10 ) {
				if ( !$this->gp->capture_list_successors( $this->offset ) ) {
					$this->gp->add_arcs( array( array( $this->offset, $this->offset+1 ) ) );
					print "fuzz offset: {$this->offset} ($gpTestGraphName)\n";
					return;
				}
				
				$this->offset ++;
			}
			
			die("no free offset left (or $gpTestGraphName needs purging)\n");
		}

		function random_node() {
			return mt_rand(10, 1000) * 10 + $this->offset;
		}
		
		function random_arcs( $n = 0 ) {
			if ( !$n ) $n = mt_rand(2, 80);
			
			$arcs= array();
			
			for ($i=0; $i<$n; $i++) {
				$a = $this->random_node();
				$b = $this->random_node();
				
				$arcs[] = array( $a, $b );
			}
			
			return $arcs;
		}

		function random_set( $n = 0 ) {
			if ( !$n ) $n = mt_rand(2, 80);
			
			$arcs= array();
			
			for ($i=0; $i<$n; $i++) {
				$x = $this->random_node();
				
				$arcs[] = $x;
			}
			
			return $arcs;
		}

		function doFuzz() {
			$this->gp->add_arcs( $this->random_arcs() );
			$this->gp->remove_arcs( $this->random_arcs() );

			$this->gp->replace_successors( $this->random_node(), $this->random_set() );
			$this->gp->replace_predecessors( $this->random_node(), $this->random_set() );
			
			return false;
		}
		
}

$fuzzer = new gpArcFuzzer( );

$fuzzer->run( $argv );
