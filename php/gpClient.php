<?php
define( 'GP_LINEBREAK', "\n" ); //XXX: should be \r\n, as per spec?!
define( 'GP_PORT', 6666 );

define( 'GP_CLIENT_PROTOCOL_VERSION', 2 );

class gpException extends Exception {
	function __construct( $msg ) {
		Exception::__construct( $msg );
	}
}

class gpProcessorException extends gpException {
	function __construct( $status, $msg, $command ) {
		if ( $command ) $msg .= " (command was `$command`)";
		
		gpException::__construct( $msg );
		
		$this->command = $command;
		$this->status = $status;
	}
}

class gpProtocolException extends gpException {
	function __construct( $msg ) {
		gpException::__construct( $msg );
	}
}

class gpUsageException extends gpException {
	function __construct( $msg ) {
		gpException::__construct( $msg );
	}
}

/////////////////////////////////////////////////////////////////////////
abstract class gpDataSource {
	public abstract function nextRow();
	
	public function close() {
		// noop
	}
}

class gpNullSource extends gpDataSource {
	public function nextRow() {
		return null;
	}
	
	static $instance;
}

gpNullSource::$instance = new gpNullSource();

class gpArraySource extends gpDataSource {
	var $data;
	var $index;
	
	public function __construct( $data ) {
		$this->data = $data;
		$this->index = 0;
	}

	public function nextRow() {
		if ( $this->index >= count($this->data) ) return null;
		
		$row = $this->data[ $this->index ];
		$this->index += 1;
		
		if ( !is_array( $row ) ) $row = array( $row );
		return $row;
	}
	
	public function makeSink() {
		return new gpArraySink( $this->data );
	}
}

class gpPipeSource extends gpDataSource {
	var $hin;
	
	public function __construct( $hin ) {
		$this->hin = $hin;
	}

	public function nextRow() {
		$s = fgets($this->hin);
		
		if ( !$s ) return null;
		
		$s = trim($s);
		if ( $s === '' ) return null;
		
		$row = gpConnection::splitRow( $s );
		return $row;
	}
}

class gpFileSource extends gpPipeSource {
	var $path;
	var $mode;
	
	public function __construct( $path, $mode = 'r' ) {
		$this->mode = $mode;
		$this->path = $path;
		
		$h = fopen( $this->path, $this->mode );
		if ( !$h ) throw new gpUsageException( "failed to open " . $this->path );
		
		gpPipeSource::__construct( $h );
	}

	public function close( ) {
		fclose( $this->hin ); 
	}
}

/////////////////////////////////////////////////////////////////////////

abstract class gpDataSink {
	public abstract function putRow( $row );
	
	public function close() {
		// noop
	}
}

class gpNullSink extends gpDataSink {
	public function putRow( $row ) {
		// noop
	}
	
	static $instance;
}

gpNullSink::$instance = new gpNullSink();

class gpArraySink extends gpDataSink {
	var $data;
	
	public function __construct( &$data = null ) {
		if ( $data ) $this->data &= $data;
		else $this->data = array();
	}
	
	public function putRow( $row ) {
		$this->data[] = $row;
	}
	
	public function getData() {
		return $this->data;
	}
	
	public function makeSource() {
		return new gpArraySource( $this->data );
	}	
}

class gpPipeSink extends gpDataSink {
	var $hout;
	
	public function __construct( $hout ) {
		$this->hout = $hout;
	}

	public function putRow( $row ) {
		$s = gpConnection::joinRow( $row );
		
		gpClient::send( $this->hout, $s . GP_LINEBREAK ); 
	}
}

class gpFileSink extends gpPipeSink {
	var $path;
	var $mode;
	
	public function __construct( $path, $append = false ) {
		if ( $append === true ) $this->mode = 'a';
		else if ( $append === false ) $this->mode = 'w';
		else $this->mode = $append;
		
		$this->path = $path;
		
		$h = fopen( $this->path, $this->mode );
		if ( !$h ) throw new gpUsageException( "failed to open " . $this->path );
		
		gpPipeSink::__construct( $h );
	}

	public function close( ) {
		fclose( $this->hout ); 
	}
}

/////////////////////////////////////////////////////////////////////////

abstract class gpConnection {
	protected $hout = null;
	protected $hin = null;
	protected $tainted = false;
	protected $closed = false;
	protected $status = null;
	protected $statusMessage = null;
	protected $response = null;
	
	public $allowPipes = false;
	public $strictArguments = true;

	public $debug = false;
	
	public abstract function connect();
	public abstract function close(); //FIXME: set $closed! //FIXME: close on quit/shutdown
	
	public function getStatus() {
		return $this->status;
	}
	
	public function getStatusMessage() {
		return $this->statusMessage;
	}
	
	public function getResponse() {
		return $this->response;
	}
	
	public function isClosed() {
		return $this->closed;
	}
	
	protected function trace( $context, $msg, $obj = 'nothing878423really' ) {
		if ( $this->debug ) {
			if ( $obj !== 'nothing878423really' ) {
				$msg .= ': ' . preg_replace( '/\s+/', ' ', var_export($obj, true) );
			}
			
			print "[gpClient] $context: $msg\n";
		}
	}
	
	public function checkPeer() {
		// noop
	} 
	
	public function getProtocolVersion() {
		$this->protocol_version();
		$version = trim($this->statusMessage);
		return $version;
	} 
	
	public function checkProtocolVersion() {
		$version = $this->getProtocolVersion();
		if ( $version != GP_CLIENT_PROTOCOL_VERSION ) throw new gpProtocolException( "Bad protocol version: expected " . GP_CLIENT_PROTOCOL_VERSION . ", but peer uses " . $version );
	} 
	
	public function ping() {
		$re = $this->protocol_version();
		$this->trace(__METHOD__, $re);
		
		return $re;
	}
	
    public function __call($name, $arguments) {
		$cmd = str_replace( '_', '-', $name);
		$cmd = preg_replace( '/^-*|-*$/', '', $cmd);
		
		$source = null;
		$sink = null;

		if ( preg_match( '/^try-/', $cmd ) ) {
			$cmd = substr( $cmd, 4 );
			$try = true;
		} else { 		
			$try = false;
		}
		
		if ( preg_match( '/^capture-/', $cmd ) ) {
			$cmd = substr( $cmd, 8 );
			$sink = new gpArraySink();
			$capture = true;
		} else { 		
			$capture = false;
		}
		
		$command = array( $cmd );
		
		foreach ( $arguments as $arg ) {
			if ( is_array( $arg ) ) {
				$source = new gpArraySource( $arg );
			} else if ( is_object( $arg ) ) {
				if ( $arg instanceof gpDataSource ) $source = $arg;
				else if ( $arg instanceof gpDataSink ) $sink = $arg;
				else throw new Exception( "arguments must be primitive or a gpDataSource or gpDataSink. Found " . get_class($arg) );
			} else if ( $arg === null || $arg === false ) {
				continue;
			} else if ( is_string($arg) || is_int($arg) ) {
				$command[] = $arg;
			} else {
				throw new Exception( "arguments must be objects, strings or integers. Found " . type($arg) );
			}
		}
		
		try {
			$status = $this->exec( $command, $source, $sink, $has_output );
		} catch ( gpProcessorException $e ) {
			if ( !$try ) throw $e;
			else return false;
		}
		
		if ( $capture ) {
			if ( $status == 'OK' ) {
				if ( $has_output ) return $sink->data;
				else return true;
			}
			else if ( $status == 'NONE' ) return null;
			else return false;
		} else {
			return $status;
		}
    }
    
    public static function send( $handle, $s ) {
		$len = strlen($s);
		
		for ($written = 0; $written < $len; $written += $c) {
			$c = fwrite($handle, substr($s, $written), $len - $written);
			
			if ($c === false) { // doc sais fwrite returns false on errors
				throw new gpProtocolException("failed to send data to peer, broken pipe! (fwrite returned false)");
			}

			if ($c === 0) { // experience sais fwrite returns 0 on errors
				throw new gpProtocolException("failed to send data to peer, broken pipe! (fwrite returned 0)");
			}
		}
		
		fflush( $handle );
		
		return $written;
	}
	
	public function setTimeout( $seconds ) {
		stream_set_timeout($this->hin, $seconds);
	}

	public function exec( $command, gpDataSource $source = null, gpDataSink $sink = null, &$has_output = null ) {
		$this->trace(__METHOD__, "BEGIN");
		
		if ( $this->tainted ) {
			throw new gpProtocolException("connection tainted by previous error!");
		}
		
		if ( $this->closed ) {
			throw new gpProtocolException("connection already closed!");
		}
		
		if ( feof( $this->hin ) ) { // closed by peer
			$this->trace(__METHOD__, "connection closed by peer, closing our side too.");
			$this->close();
			$this->tainted = true;
			
			throw new gpProtocolException("connection closed by peer!");
		}
		
		if ( is_array( $command ) ) {
			if ( !$command ) {
				throw new gpUsageException("empty command!");
			}
			
			$c = $command[0];
			if ( is_array( $c ) || is_object( $c ) ) throw new gpUsageException("invalid command type: " . gettype($c));
				
			if ( !self::isValidCommandName( $c ) ) {
				throw new gpUsageException("invalid command name: " . $c);
			}
			
			$strictArgs = $this->strictArguments;
			foreach ( $command as $c ) {
				if ( is_array( $c ) || is_object( $c ) ) throw new gpUsageException("invalid argument type: " . gettype($c));
				
				if ( $this->allowPipes && preg_match('/^[<>]$/', $c) ) $strictArgs = false; // pipe, allow lenient args after that
				if ( $this->allowPipes && preg_match('/^[|&!:<>]+$/', $c) ) continue; //operator
				
				if ( !self::isValidCommandArgument($c, $strictArgs) ) throw new gpUsageException("invalid argument: $c");
			}
			
			$command = implode( ' ', $command );
		} 
		
		$command = trim($command);
		if ($command == '') throw new gpUsageException("command is empty!");

		$this->trace(__METHOD__, "command", $command );

		if ( !self::isValidCommandString($command) ) throw new gpUsageException("invalid command: $command");

		if ( !$this->allowPipes && preg_match('/[<>]/', $command) ) throw new gpUsageException("command denied, pipes are disallowed by allowPipes = false; command: $command");
		
		if ( $source && !preg_match('/:$/', $command) ) {
			$command .= ':';
		}
		
		if ( !$source && preg_match('/:$/', $command) ) {
			$source = gpNullSource::$instance;
		}
		
		if ( $source && preg_match('/</', $command) ) {
			throw new gpUsageException("can't use data input file and a local data source at the same time! $command");
		}

		if ( $sink && preg_match('/>/', $command) ) {
			throw new gpUsageException("can't use data output file and a local data sink at the same time! $command");
		}
		
		$this->trace(__METHOD__, ">>> ", $command);
		gpClient::send( $this->hout, $command . GP_LINEBREAK ); 
		
		$this->trace(__METHOD__, "source", $source == null ? null : get_class($source));
		
		if ( $source ) {
			$this->copyFromSource( $source );
		}
		
		$re = fgets( $this->hin, 1024 ); //FIXME: what if response is too long?
		$this->trace(__METHOD__, "<<< ", $re);
		
		if ( $re === '' || $re === false || $re === null ) {
			$this->tainted = true;
			$this->status = null;
			$this->statusMessage = null;
			$this->response = null;
			
			$this->trace(__METHOD__, "peer did not respond! Got value " . var_export($re, true));
			$this->checkPeer();
			
			throw new gpProtocolException("peer did not respond! Got value " . var_export($re, true));
		}
		
		$re = trim($re);

		$this->response = $re;
			
		preg_match( '/^([a-zA-Z]+)[.:!](.*?):?$/', $re, $m );
		if ( empty( $m[1] ) ) {
			$this->tainted = true;
			$this->close();
			throw new gpProtocolException("response should begin with status string like `OK`. Found: `$re`");
		}
		
		$this->status = $m[1];
		$this->statusMessage = trim($m[2]);
		
		if ( $this->status != 'OK' && $this->status != 'NONE' ) {
			throw new gpProcessorException( $this->status, $m[2], $command );
		}
		
		$this->trace(__METHOD__, "sink", $sink == null ? null : get_class($sink));
		
		if ( preg_match( '/: *$/', $re ) ) {
			if ( !$sink ) $sink = gpNullSink::$instance; //note: we need to slurp the result in any case!
			$this->copyToSink( $sink );
			
			$has_output = true;
		} else {
			$has_output = false;
		}
		
		if ( feof( $this->hin ) ) { // closed by peer
			$this->trace(__METHOD__, "connection closed by peer, closing our side too.");
			$this->close();
		}
		
		return $this->status;
	}
	
	public static function isValidCommandName( $name ) {
		return preg_match('/^[a-zA-Z_][-\w]*$/', $name);
	}
	
	public static function isValidCommandString( $command ) {
		if ( !preg_match('/^[a-zA-Z_][-\w]*($|[\s!&|<>#:])/', $command) ) return false; // must start with a valid command
		
		return !preg_match('/[\0-\x1F\x80-\xFF]/', $command);
	}
	
	public static function isValidCommandArgument( $arg, $strict = true ) {
		if ( $arg === '' || $arg === false || $arg === null ) return false;

		if ( $strict ) return preg_match('/^\w[-\w]*(:\w[-\w]*)?$/', $arg); //XXX: the ":" is needed for user:passwd auth. not pretty. 
		
		return !preg_match('/[\0-\x1F\x80-\xFF:|<>!&#]/', $arg); //low chars, high chars, and operators.
	}
	
	public static function splitRow( $s ) {
		if ( $s === '' ) return false;
		if ( $s === false || $s === null ) return false;
		
		if ( $s[0] == '#' ) {
			$row = array( substr($s, 1) );
		} else {
			$row = preg_split( '/ *[;,\t] */', $s );
			
			foreach ( $row as $i => $v ) {
				if ( preg_match('/^\d+$/', $v) ) {
					$row[$i] = (int) $v;
				}
			}
		}
		
		return $row;
	}

	public static function joinRow( $row ) {
		if ( empty($row) ) return '';
		
		if ( is_string( $row ) ) {
			return '#' . $row;
		}
		
		if ( count( $row ) === 1 &&  is_string($row[0]) 
				&& !preg_match( '/^\d+$/', $row[0] ) ) {
			
			return '#' . $row[0];
		}
		
		$s = implode(',', $row);
		return $s;
	}
	
	protected function copyFromSource( gpDataSource $source ) {
		$sink = new gpPipeSink( $this->hout );
		
		$this->trace(__METHOD__, "source", get_class($source));
		
		$this->copy( $source, $sink, ' > ' );

		// $source->close(); // to close or not to close...

		gpClient::send( $this->hout, GP_LINEBREAK ); 

		$this->trace(__METHOD__, "copy complete.");

		/*
		while ( $row = $source->nextRow() ) {
			$s = gpConnection::joinRow( $row );
			
			fputs($this->hout, $s . GP_LINEBREAK);
		}

		fputs($this->hout, GP_LINEBREAK); // blank line
		*/
	}

	protected function copyToSink( gpDataSink $sink = null ) {
		$source = new gpPipeSource( $this->hin );
		
		$this->trace(__METHOD__, "sink", get_class($sink));
		
		$this->copy( $source, $sink, ' < ' );

		$this->trace(__METHOD__, "copy complete.");
		
		// $source->close(); // to close or not to close...
		
		/*
		while ( $s = fgets($this->hin) ) {
			$s = trim($s);
			if ( $s === '' ) break;
			
			$row = gpConnection::splitRow( $s );
			
			if ( $sink ) {
				$sink->putRow( $row );
			}
		} */
	}
	
	public function copy( gpDataSource $source, gpDataSink $sink = null, $indicator = '<>' ) {
		while ( $row = $source->nextRow() ) {
			if ( $sink) {
				$sink->putRow( $row );
				$this->trace(__METHOD__, $indicator, $row);
			} else {
				$this->trace(__METHOD__, "#", $row);
			}
		}
	}
	 
}

class gpClient extends gpConnection {
	var $host;
	var $port;
	var $graphname;
	var $socket = false;

	public function __construct( $graphname, $host = false, $port = false ) {
		if ( $host === false ) $host = 'localhost';
		if ( $port === false ) $port = GP_PORT;

		$this->port = $port;
		$this->host = $host;
		$this->graphname = $graphname;
	}
	
	public function connect() {
		$this->socket = @fsockopen($this->host, $this->port, $errno, $errstr); //XXX: configure timeout?
		
		if ( !$this->socket ) throw new gpProtocolException( "failed to connect to " . $this->host . ":" . $this->port . ': ' . $errno . ' ' . $errstr );
		
		$this->hin = $this->socket;
		$this->hout = $this->socket;
		
		if ( $this->graphname ) {
			try {
				$this->use_graph($this->graphname);
			} catch ( gpException $e ) {
				$this->close();
				throw $e;
			}
		}
		
		$this->checkProtocolVersion();
		
		return true;
	}
	
	public function close() {
		if ( !$this->socket ) return false;
		
		@fclose( $this->socket );

		$this->process = false;
		$this->closed = true;
	}
}

class gpSlave extends gpConnection {
	var $process;
	var $command;

	public function __construct( $command, $cwd = null, $env = null ) {
		$this->command = $command;
		
		$this->cwd = $cwd;
		$this->env = $env;
	}

	public function makeCommand( $command ) {
		if ( empty( $command ) ) throw new Exception('empty command given');
		
		$path = null;
		if ( is_array( $command ) ) {
			foreach ( $command as $i => $arg ) {
				if ( $i === 0) {
					$cmd = escapeshellcmd( $arg );
					$path = $args;
				} else {
					$cmd .= ' ' . escapeshellarg( $arg );
				}
			}
		} else {
			if ( preg_match( '!^ *([-_a-zA-Z0-9.\\\\/]+)( [^"\'|<>]$|$)!', $command, $m ) ) { // extract path from simple command
				$path = $m[1];
			}
			
			$cmd = trim($command);
		}
		
		if ( $path ) {
			if ( !file_exists( $path) ) throw new gpUsageException('file does not exist: ' . $path);
			if ( !is_readable( $path) ) throw new gpUsageException('file does not readable: ' . $path);
			if ( !is_executable( $path) ) throw new gpUsageException('file does not executable: ' . $path);
		}
		
		return $cmd;
	}
	
	public function connect() {
		$cmd = $this->makeCommand( $this->command );
		
		$descriptorspec = array(
		   0 => array("pipe", "r"),  // stdin is a pipe that the child will read from
		   1 => array("pipe", "w"),  // stdout is a pipe that the child will write to
		   2 => array("pipe", "w"),  // XXX: nothing should ever go to stderr... but what if it does?
		);

		$this->process = proc_open($cmd, $descriptorspec, $pipes, $this->cwd, $this->env);
		
		if ( !$this->process ) {
			$this->trace(__METHOD__, "failed to execute " . $this->command );
			throw new gpProtocolException( "failed to execute " . $this->command );
		}

		$this->trace(__METHOD__, "executing command " . $this->command . " as " . $this->process );
		
		$this->hin = $pipes[1];
		$this->hout = $pipes[0];
		//XXX: what about stderr ?!

		$this->trace(__METHOD__, "reading from " . $this->hin );
		$this->trace(__METHOD__, "writing to " . $this->hout );

		usleep( 100 * 1000 ); //XXX: NASTY HACK! wait 1/10th of a second to see if the command actually starts
		
		$this->checkProtocolVersion();

		return true;
	}
	
	public function close() {
		if ( !$this->process ) return false;
		
		@proc_close( $this->process );

		$this->process = false;
		$this->closed = true;
	}
	
	public function checkPeer() {
		$status = proc_get_status( $this->process );
		
		$this->trace(__METHOD__, "status", $status );

		if ( !$status['running'] ) throw new gpProtocolException('slave process is not running! exit code ' . $status['exitcode'] ); 
	} 
	
	
}

function array_column($a, $col) {
	$column = array();
	
	foreach ( $a as $k => $x ) {
		$column[$k] = $x[$col];
	}
	
	return $column;
}

function pairs2map( $pairs ) {
	$map = array();
	
	foreach ( $pairs as $p ) {
		$map[ $p[0] ] = $p[1];
	}
	
	return $map;
}
	
	
