<?php
require_once('../gpClient.php');
require_once('gpTestConfig.php');

$gpTestGraphName = 'test' . getmypid();

function fuzz_pick( $a ) {
	$i = mt_rand(0, count($a)-1 );
	return $a[$i];
}

/**
 * Tests the TCP client connection
 */
abstract class gpFuzzerBase 
{
	
	var $graph = null;
	var $useTempGraph = true;
	
	public function newConnection() {
		global $gpTestGraphServHost, $gpTestGraphServPort;

		$gp = gpConnection::new_client_connection( null, $gpTestGraphServHost, $gpTestGraphServPort );
		$gp->connect();

		return $gp;
	}
	
		function connect() {
			global $gpTestAdmin, $gpTestAdminPassword;
			global $gpTestGraphName;
			global $gpTestGraphServHost, $gpTestGraphServPort;
			
			if ( !$this->graph ) {
				$this->graph = $gpTestGraphName;
			}
			
			try {
				$this->gp = $this->newConnection(); 
			} catch ( gpException $ex ) {
				print("Unable to connect to $gpTestGraphServHost:$gpTestGraphServPort, please make sure the graphserv process is running and check the \$gpTestGraphServHost and \$gpTestGraphServPort configuration options in gpTestConfig.php.\nOriginal error: " . $ex->getMessage() . "\n");
				exit(11);
			}
			
			try {
				$this->gp->authorize( 'password', "$gpTestAdmin:$gpTestAdminPassword" );
			} catch ( gpException $ex ) {
				print("Unable to connect to authorize as $gpTestAdmin, please check the \$gpTestAdmin and \$gpTestAdminPassword configuration options in gpTestConfig.php.\nOriginal error: " . $ex->getMessage() . "\n");
				exit(12);
			}

			if ( $this->useTempGraph ) {
					$this->gp->try_create_graph( $this->graph );
			}

			try {
				$this->gp->use_graph( $this->graph );
			} catch ( gpException $ex ) {
				print("Unable to use graphe {$this->graph}, please check the \$gpTestGraphName configuration option in gpTestConfig.php as well as the privileges of user $gpTestAdmin.\nOriginal error: " . $ex->getMessage() . "\n");
				exit(13);
			}
		}
		
		function disconnect() {
			global $gpTestAdmin, $gpTestAdminPassword;
			
			if ( $this->useTempGraph && $this->graph ) {
				$this->try_drop_graph($this->graph);
			}
		}
		
		function prepare() {
		}
		
		abstract function doFuzz();


		function run($argv) {
			$this->connect();
			$this->prepare();

			$n = null;
			if ( count($argv) > 1 ) {
				$n = (int) $argv[1];
			}

			if (!$n) $n = 100;

			for ( $k=0; $k<$n; $k++ ) {
				for ( $i=0; $i<100; $i++ ) {
					$ok = $this->doFuzz();
					
					if ( $ok ) print "+";
					else print "-";
				}

				print "\n";
				sleep(1);
			}

			$this->disconnect();
		}
}
