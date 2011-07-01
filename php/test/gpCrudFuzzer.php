<?php
require_once('gpFuzzerBase.php');

$fuzz_commands = array(
	"list-roots",
	"list-successors",
	"add-arcs",
	"stats",
	"22",
	"-11111",
	"xyz",
	"0",
	"!",
	"&",
	"%",
	"#",
	"",
);

$fuzz_args = array(
	"1",
	"2",
	"11",
	"22",
	"-11111",
	"xyz",
	"0",
	"!",
	"&",
	"%",
	"#",
	"",
);


/**
 * Tests the TCP client connection
 */
class gpCrudFuzzer extends gpFuzzerBase 
{

		function prepare() {
			$this->gp->add_arcs( array(
				array(1, 2),
				array(1, 11),
				array(2, 22),
			));
		}

		function doFuzz() {
			global $fuzz_commands;
			global $fuzz_args;
			
			$cmd = "";

			$cmd .= fuzz_pick($fuzz_commands);
			$cmd .= " ";
			$cmd .= fuzz_pick($fuzz_args);
			$cmd .= " ";
			$cmd .= fuzz_pick($fuzz_args);
			
			try {
				$this->gp->exec($cmd);
				return true;
			} catch ( gpProcessorException $ex ) {
				//noop
			} catch ( gpUsageException $ex ) {
				//noop
			}
			
			return false;
		}
		
}

$fuzzer = new gpCrudFuzzer();

$fuzzer->run( $argv );
