<?php
define( 'GP_NEWLINE', "\r\n" );
define( 'GP_PORT', 6699 );

class gpException extends Exception {
	function __construct( $msg ) {
		Exception::__construct( $msg );
	}
}

class gpProcessorException extends gpException {
	function __construct( $status, $msg ) {
		gpException::__construct( $msg );
		
		$this->status = $status;
	}
}

class gpProtocolException extends gpException {
	function __construct( $msg ) {
		gpException::__construct( $msg );
	}
}

/////////////////////////////////////////////////////////////////////////
class gpDataSource {
	public abstract function nextRow();
	
	public function close() {
		// noop
	}
}

class gpNullSource extends gpDataSource {
	public function nextRow() {
		return null;
	}
}

class gpArraySource extends gpDataSource {
	var $data;
	var $index;
	
	public __construct($data, ) {
		$this->data = $data;
		$this->index = 0;
	}

	public function nextRow() {
		if ( $this->index >= count($this->data) ) return null;
		
		$row = $this->data[ $this->index ];
		$this->index += 1;
		
		return $row;
	}
}

class gpPipeSource extends gpDataSource {
	var $hin;
	
	public __construct( $hin ) {
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
	
	public __construct( $path, $mode = 'r' ) {
		$this->mode = $mode;
		$this->path = $path;
		
		$h = fopen( $this->path, $this->mode );
		if ( !$h ) throw new gpException( "failed to open " . $this->path );
		
		gpPipeSource::__construct( $h );
	}

	public function close( ) {
		fclose( $this->hin ); 
	}
}

/////////////////////////////////////////////////////////////////////////

class gpDataSink {
	public abstract function putRow( $row );
	
	public function close() {
		// noop
	}
}

class gpNullSink extends gpDataSink {
	public function putRow( $row ) {
		// noop
	}
}

class gpArraySink extends gpDataSink {
	var $data;
	
	public __construct() {
		$this->data = array();
	}
	
	public function putRow( $row ) {
		$this->data[] = $row;
	}
	
	public function getData() {
		return $this->data;
	}
}

class gpPipeSink extends gpDataSink {
	var $hout;
	
	public __construct( $hout ) {
		$this->hout = $hout;
	}

	public function putRow( $row ) {
		$s = gpConnection::joinRow( $row );
		
		fputs($this->hout, $s . GP_LINEBREAK); 
	}
}

class gpFileSink extends gpPipeSink {
	var $path;
	var $mode;
	
	public __construct( $path, $append ) {
		if ( $append === true ) $this->mode = 'a';
		else if ( $append === false ) $this->mode = 'w';
		else $this->mode = $append;
		
		$this->path = $path;
		
		$h = fopen( $this->path, $this->mode );
		if ( !$h ) throw new gpException( "failed to open " . $this->path );
		
		gpPipeSink::__construct( $h );
	}

	public function close( ) {
		fclose( $this->hout ); 
	}
}

/////////////////////////////////////////////////////////////////////////

class gpConnection {
	var $hout;
	var $hin;
	var $tainted;
	var $closed;

	public abstract function connect();
	public abstract function close(); //FIXME: set $closed! //FIXME: close on quit/shutdown

	public static function splitRow( $s ) {
		if ( $s === '' ) return false;
		if ( $s === false || $s === null ) return false;
		
		if ( $s[0] == '#' ) {
			$row = array( substr($s, 1) );
		} else {
			$row = preg_split( '/ *[;,\t] */', $s );
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

	public function exec( $command, gpDataSource $source = null, gpDataSink $sink = null ) {
		if ( $this->tainted ) {
			throw new gpProtocolException("connection tainted by previous error!");
		}
		
		if ( $this->closed ) {
			throw new gpProtocolException("connection already closed!");
		}
		
		if ( is_array( $command ) ) {
			$command = implode( ' ', $command );
		}
		
		fputs( $this->hout, trim($command) . "\n" ); #FIXME: "\r\n"???
		fflush( $this->hout );
		
		if ( $source ) $this->copyFromSource( $source );
		
		$re = fgets( $this->hin, 1024 ); //FIXME: what if response is too long?
		
		preg_match( '/^([a-zA-Z]+)[.:!](.*)$/', $re, $m );
		if ( empty( $m[1] ) ) {
			$this->tainted = true;
			//FIXME: should we close it??
			throw new gpProtocolException("response should begin with status string like `OK`");
		}
		
		if ( $m[1] != 'OK' ) throw new gpProcessorException( $m[1], $m[2] );

		preg_match( '/.*([.:]) *$/', $re, $m );
		if ( empty( $m[1] ) ) throw new gpProtocolException("response should end with `.` or `:`");
		
		if ( $m[1] == ':' ) $this->copyToSink( $sink );
	}
	
	protected function copyFromSource( gpDataSource $source ) {
		$sink = new gpPipeSink( $this->hout );
		
		gpConnection::copy( $source, $sink );

		// $source->close(); // to close or not to close...

		fputs($this->hout, GP_LINEBREAK); // blank line
		fflush( $this->hout );

		/*
		while ( $row = $source->nextRow() ) {
			$s = gpConnection::joinRow( $row );
			
			fputs($this->hout, $s . GP_LINEBREAK);
		}

		fputs($this->hout, GP_LINEBREAK); // blank line
		*/
	}

	protected function copyToSink( gpDataSink $sink ) {
		$source = new gpPipeSource( $this->hin );
		
		gpConnection::copy( $source, $sink );

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
	
	public static function copy( gpDataSource $source, gpDataSink $sink ) {
		while ( $row = $source->nextRow() ) {
			$sink->putRow( $row );
		}
	}
	 
}

class gpServerConnection {
	var $host;
	var $port;
	var $graphname;

	public __construct( $graph, $host = false, $port = false ) {
		if ( $host === false ) $host = 'localhost';
		if ( $port === false ) $port = GP_PORT;

		$this->port = $port;
		$this->host = $host;
		$this->graphname = $graphname;
	}

	public function connect() {
		........
	}
	
	public function close() {
		........
	}
}

class gpSlaveConnection {
	var $process;
	var $command;

	public __construct( $command, $cwd = null, $env = null ) {
		$this->command = $command;
		
		$this->cwd = $cwd;
		$this->env = $env;
	}

	public function connect() {
		$descriptorspec = array(
		   0 => array("pipe", "r"),  // stdin is a pipe that the child will read from
		   1 => array("pipe", "w"),  // stdout is a pipe that the child will write to
		   2 => array("pipe", "w"),  // XXX: nothing should ever go here... but what if it does?
		);

		$this->process = proc_open($this->command, $descriptorspec, $pipes, $this->cwd, $this->env);
		
		if ( !$this->process ) throw new qpException( "failed to execute " . $this->command );
		
		$this->hin = $pipes[1];
		$this->hout = $pipes[0];
		//XXX: what about stderr ?!

		return true;
	}
	
	public function close() {
		if ( !$this->process ) return false;
		
		@pclose( $this->process );

		$this->process = false;
		$this->closed = true;
	}
}
