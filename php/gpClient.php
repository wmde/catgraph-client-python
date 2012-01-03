<?php
/*
  Graph Processor Client Library by Daniel Kinzler
  Copyright (c) 2011 by Wikimedia Deutschland e.V.
  All rights reserved.
 
  Redistribution and use in source and binary forms, with or without
  modification, are permitted provided that the following conditions are met:
      * Redistributions of source code must retain the above copyright
        notice, this list of conditions and the following disclaimer.
      * Redistributions in binary form must reproduce the above copyright
        notice, this list of conditions and the following disclaimer in the
        documentation and/or other materials provided with the distribution.
      * Neither the name of Wikimedia Deutschland nor the
        names of its contributors may be used to endorse or promote products
        derived from this software without specific prior written permission.
 
  THIS SOFTWARE IS PROVIDED BY WIKIMEDIA DEUTSCHLAND ''AS IS'' AND ANY
  EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE IMPLIED
  WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE ARE
  DISCLAIMED. IN NO EVENT SHALL WIKIMEDIA DEUTSCHLAND BE LIABLE FOR ANY
  DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES
  (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES;
  LOSS OF USE, DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND
  ON ANY THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
  (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE OF THIS
  SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
  
 
 NOTE: This software is not released as a product. It was written primarily for
 Wikimedia Deutschland's own use, and is made public as is, in the hope it may
 be useful. Wikimedia Deutschland may at any time discontinue developing or
 supporting this software. There is no guarantee any new versions or even fixes
 for security issues will be released.
 
 */

/**
 * The Graph Processor Client Library is a PHP interface to a GraphServ
 * or GraphCore instance. 
 *
 * @author    Daniel Kinzler <daniel.kinzler@wikimedia.de>
 * @copyright 2011, Wikimedia Deutschland
 * 
 * @package WikiTalk
 */

/** 
 * Linebreak to use when talking to GraphServ or GraphCore instances.
 * This is \r\n per spec. but \n alone should also work. 
 **/
define( 'GP_LINEBREAK', "\r\n" ); 

/**
 * Default GraphServ port
 */
define( 'GP_PORT', 6666 );

/**
 * Implemented GraphServ protocol version. This indicates what features the
 * client library supports. It is not validated against the peer's protocol version,
 * see GP_MIN_PROTOCOL_VERSION and GP_MAX_PROTOCOL_VERSION for that.
 */
define( 'GP_CLIENT_PROTOCOL_VERSION', 4 ); 

/**
 * Lowest acceptable GraphServ protocol version. If GraphServ (resp GraphCore)
 * reports a lower protocol version, the conenction is aborted.
 */
define( 'GP_MIN_PROTOCOL_VERSION', 2.0 ); 

/**
 * Highest acceptable GraphServ protocol version. If GraphServ (resp GraphCore)
 * reports a higher protocol version, the conenction is aborted.
 */
define( 'GP_MAX_PROTOCOL_VERSION', 4.99 );

/**
 * Base class for gpClient exceptions.
 */
class gpException extends Exception {
	function __construct( $msg ) {
		Exception::__construct( $msg );
	}
}

/**
 * Exception representing an error reported by the remote GraphServ or 
 * GraphCore instance.
 */
class gpProcessorException extends gpException {
	function __construct( $status, $msg, $command ) {
		if ( $command ) $msg .= " (command was `$command`)";
		
		gpException::__construct( $msg );
		
		$this->command = $command;
		$this->status = $status;
	}
}

/**
 * Exception for reporting failures in communicating with the remote 
 * GraphServ or GraphCore instance.
 */
class gpProtocolException extends gpException {
	function __construct( $msg ) {
		gpException::__construct( $msg );
	}
}

/**
 * Exception raised when gpClient encounters a problem on the client side.
 */
class gpClientException extends gpException {
	function __construct( $msg ) {
		gpException::__construct( $msg );
	}
}

/**
 * Exception raised with gpClient is used incorrectly.
 */
class gpUsageException extends gpClientException {
	function __construct( $msg ) {
		gpException::__construct( $msg );
	}
}

/////////////////////////////////////////////////////////////////////////

/**
 * Base class for all "data sources". A data source is an object that offers a
 * nextRow() method, which returns one row of data after another. Essentially,
 * gpDataSource represents an interator of rows in a tabular data set.
 * 
 * Data sources are used in the gpClient framework to represent origin of a 
 * data transfer. Typically, a data source is used to provide data to a 
 * GraphCore command, such as add_arcs.
 */
abstract class gpDataSource {
	
	/**
	 * Returns the next row. The row is represented as an indexed array.
	 * Successive calls to nextRow() on the same data source should return
	 * rows of the same size, with the same array keys. When there is no more
	 * data in the data source, nextRow() shall return null.
	 * 
	 * @return an array representing hte next row, or null of the end of the data
	 * source has been reached.
	 */
	public abstract function nextRow();
	
	/**
	 * Closes the data source and frees any resources allocated by this object. 
	 * close() should always be called when a data source is no longer needed,
	 * usually on the same level as the data source object was created.
	 * 
	 * After close() has been called on a data source object,
	 * the behavior of calling nextRow() on that object is undefined.
	 */
	public function close() {
		// noop
	}
}

/**
 * An empty data source.
 */
class gpNullSource extends gpDataSource {
	
	/**
	 * always returns null.
	 */
	public function nextRow() {
		return null;
	}
	
	/**
	 * singleton instance of gpNullSource
	 */
	static $instance;
}

gpNullSource::$instance = new gpNullSource(); // initialize singleton instance

/**
 * A data source that iterates over an array. This is useful to use 
 * programmatically generated data as the input to some GraphCore command.
 * 
 * The gpArraySource maintains a current index pointing into the data array.
 * Every call to nextRow() increments that index to the next row.
 */
class gpArraySource extends gpDataSource {
	var $data;
	var $index;
	
	/**
	 * Initializes a gpArraySource from the tabular data contain in $data.
	 * 
	 * @param array $data an array of indexed arrays, each representing a row 
	 *        in the data source.
	 */
	public function __construct( $data ) {
		$this->data = $data;
		$this->index = 0;
	}

	/**
	 * Returns the next row from the array provided to the constructor.
	 * If the current index points beyond the end of the array, this method 
	 * returns null to signal the end of the data source.
	 */
	public function nextRow() {
		if ( $this->index >= count($this->data) ) return null;
		
		$row = $this->data[ $this->index ];
		$this->index += 1;
		
		if ( !is_array( $row ) ) $row = array( $row );
		return $row;
	}
	
	/**
	 * Returns a new instance of gpArraySink which can be used to write to and fill the
	 * data array of this gpArraySource.
	 */
	public function makeSink() {
		return new gpArraySink( $this->data );
	}
}

/**
 * Data source based on a file handle. Each line read from the file
 * handle is interpreted as (and converted to) a data row.
 * 
 * Note: calling close() on a gpPipeSource does *not* close the
 * underlying file handle. The idea is that the handle should be closed
 * by the same code that also opened the file.
 */
class gpPipeSource extends gpDataSource {
	var $hin;
	
	/**
	 * Initializes a new gpPipeSource
	 * 
	 * @param resource $hin a handle of an open file that allows read access,
	 *        as returned by fopen or fsockopen.
	 */
	public function __construct( $hin ) {
		$this->hin = $hin;
	}

	/**
	 * Reads one line from the file handle (using fgets). If EOF is found,
	 * this method returns null. Otherwise, the line is split using
	 * gpConnection::splitRow() and the result is returned as the next
	 * row.
	 * 
	 * @return array the next data row, extracted from the next line read
	 *         from the file handle.
	 */
	public function nextRow() {
		$s = fgets($this->hin);
		
		if ( !$s ) return null;
		
		$s = trim($s);
		if ( $s === '' ) return null;
		
		$row = gpConnection::splitRow( $s );
		return $row;
	}
}

/**
 * Data source based on reading from a file. Extends gpPipeSource to handle
 * an actual local file.
 * 
 * Note: calling close() on a gpFileSource *does* close the
 * underlying file handle. The idea is that the handle should be closed
 * by the same code that also opened the file.
 */
class gpFileSource extends gpPipeSource {
	var $path;
	var $mode;
	
	/**
	 * Creates a data source for reading from the given file. The file
	 * is opened using fopen($path, $mode).
	 * 
	 * @param string $path the path of the file to read from
	 * @param string $mode (default: 'r') the mode with which the file should be opened.
	 * 
	 * @throws gpClientException if fopen failed to open the file given by $path.
	 */
	public function __construct( $path, $mode = 'r' ) {
		$this->mode = $mode;
		$this->path = $path;
		
		$h = fopen( $this->path, $this->mode );
		if ( !$h ) throw new gpClientException( "failed to open " . $this->path );
		
		gpPipeSource::__construct( $h );
	}

	/**
	 * Closes the file handle.
	 */
	public function close( ) {
		fclose( $this->hin ); 
	}
}

/////////////////////////////////////////////////////////////////////////

/**
 * Base class for "data sinks". The gpClient framework uses data sink objects to
 * represent the endpoint of a data transfer. That is, a data sink accepts one row
 * of tabular data after another, and handles them in some way.
 */
abstract class gpDataSink {
	
	/**
	 * Handles the given row in some way. How the row is processed is specific
	 * to the concrete implementation. 
	 */
	public abstract function putRow( $row );
	
	/**
	 * In case any output has been buffered (or some other kind of action has been deferred),
	 * it should be written now (resp. deferred actions should be performed and made permanent).
	 * 
	 * The default implementation of this method does nothing. Any subclass that
	 * applies any kind of buffereing to the output should override it to make all
	 * pending changes permanent.
	 */
	public function flush() {
		// noop
	}

	/**
	 * Closes this data output and releases any resources this object may have allocated.
	 * The behavior of calls to putRow() is undefined after close() was called on the
	 * same object.
	 * 
	 * The default implementation of this method calls flush(). Any subclass that
	 * allocate any external resources should override this method to release those
	 * resources.
	 */
	public function close() {
		$this->flush();
	}
}

/**
 * A data sink that simply ignores all incoming data.
 */
class gpNullSink extends gpDataSink {
	
	/**
	 * does nothing. 
	 */
	public function putRow( $row ) {
		// noop
	}
	
	/**
	 * singleton instance of gpNullSink
	 */
	static $instance;
}

gpNullSink::$instance = new gpNullSink(); // initialize singleton instance

/**
 * A data sink that appends each row to a data array. This is typically used to make
 * the data returned from a GraphCore command available for programmatic processing.
 * It should however not be used in situations where large amounts of data are 
 * expected to be returned.
 */
class gpArraySink extends gpDataSink {
	var $data;
	
	/**
	 * Initializeses a new gpArraySink
	 * 
	 * @param array $data (optional) a reference to the array the rows
	 *        should be appended to. If not given, a new array will be created,
	 *        and can be accessed using the getData() method.
	 */
	public function __construct( &$data = null ) {
		if ( $data ) $this->data &= $data;
		else $this->data = array();
	}
	
	/**
	 * Appends the given row to the array of tabular data maintained by 
	 * this gpArraySink. The data can be accessed using the getData() method-
	 */
	public function putRow( $row ) {
		$this->data[] = $row;
	}
	
	/**
	 * Returns the array that contains this gpArraySink's tabular data.
	 * This method is typically used to access the data collected by this
	 * data sink.
	 */
	public function getData() {
		return $this->data;
	}
	
	/**
	 * Returns a new instance of gpArraySource that may be used to read the rows
	 * from the array of tabular data maintained by this gpArraySink.
	 */
	public function makeSource() {
		return new gpArraySource( $this->data );
	}	
	
	/**
	 * Returns the tabular data maintained by this data sink as an associative 
	 * array (aka a map) of key value associations. This only works for two 
	 * column data, where each oclumn is interpreted as a pair of key and value.
	 * The first collumn is used as the key and the second column is used as 
	 * the value.
	 * 
	 * @return array an associative array created under the assumption that
	 *         the tabular data in this gpArraySink consists of key value pairs.
	 */
	public function getMap() {
		return pairs2map($this->data);
	}
}

/**
 * Data sink based on a file handle. Each data row is written as a line
 * to the file handle.
 * 
 * Note: calling close() on a gpPipeSink does *not* close the
 * underlying file handle. The idea is that the handle should be closed
 * by the same code that also opened the file.
 */
class gpPipeSink extends gpDataSink {
	var $hout;
	var $linebreak;
	
	/**
	 * Initializes a new pipe sink with the given file handle.
	 * 
	 * @param resource $hout a file handle that can be written to, such as 
	 *        returned by fopen or fsockopen.
	 * @param string $linebreak character(s) to use to separate rows in the
	 *        output (default: GP_LINEBREAK)
	 */
	public function __construct( $hout, $linebreak = null ) {
		if ( !$linebreak ) $linebreak = GP_LINEBREAK;
		
		$this->hout = $hout;
		$this->linebreak = $linebreak;
	}

	/**
	 * Writes the given data row to the file handle. gpConnection::joinRow() is
	 * used to encode the data row into a line of text. gpPipeTransport::send_to()
	 * is used to write the line to the file handle.
	 * 
	 * Note that the rows passed to successive calls to putRow() should
	 * have the same number of fields and use the same array keys.
	 * 
	 * @param array $row an indexed array representing a data row.
	 */
	public function putRow( $row ) {
		$s = gpConnection::joinRow( $row );

		#print "--- $s\n";
		gpPipeTransport::send_to( $this->hout, $s . $this->linebreak ); 
	}
	
	/**
	 * Flushes any pending data on the file handle (using fflush).
	 */
	public function flush() {
		fflush( $this->hout );
	}
}

/**
 * Data sink based on writing to a file. Extends gpPipeSink to handle
 * an actual local file.
 * 
 * Note: calling close() on a gpFileSink *does* close the
 * underlying file handle. The idea is that the handle should be closed
 * by the same code that also opened the file.
 */
class gpFileSink extends gpPipeSink {
	var $path;
	var $mode;
	
	/**
	 * Creates a new gpFileSink around the given file. The file given by $path 
	 * is opened using fopen.
	 * 
	 * @param string $path the path to the local file to write to.
	 * @param boolean $append whether to append to the file, or override it
	 * @param string $linebreak character(s) to use to separate lines in the
	 *        resulting file (default: PHP_EOL)
	 * 
	 * @throws gpClientException if the file could not be opened.
	 */
	public function __construct( $path, $append = false, $linebreak = null ) {
		if ( $append === true ) $this->mode = 'a';
		else if ( $append === false ) $this->mode = 'w';
		else $this->mode = $append;
		
		if ( !$linebreak ) $linebreak = PHP_EOL;
		
		$this->path = $path;
		
		$h = fopen( $this->path, $this->mode );
		if ( !$h ) throw new gpClientException( "failed to open " . $this->path );
		
		gpPipeSink::__construct( $h, $linebreak );
	}

	/**
	 * closes the file handle (after flushing it).
	 */
	public function close( ) {
		parent::close();
		fclose( $this->hout ); 
	}
}

/////////////////////////////////////////////////////////////////////////

/**
 * Base class of all transports used by the gpCleint framework. A transport
 * abstracts the way the framework communicates with the remote peer (i.e. the 
 * instance of GraphServ resp. GraþhCore). It also implements to logic to connect to the
 * remote instance.
 */
abstract class gpTransport {
	protected $closed = false;
	protected $debug = false;

	protected function trace( $context, $msg, $obj = 'nothing878423really' ) {
		if ( $this->debug ) {
			if ( $obj !== 'nothing878423really' ) {
				$msg .= ': ' . preg_replace( '/\s+/', ' ', var_export($obj, true) );
			}
			
			print "[gpTransport] $context: $msg\n";
		}
	}

	/**
	 * Returns true if this gptransport is closed, by calling close() or for 
	 * some other reason.
	 */
	public function isClosed() {
		return $this->closed;
	}
	
	/**
	 * Closes this gpTransport, disconnecting from the peer and freeing any
	 * resources that this object may have allocated.
	 * 
	 * After close() has been called, isClosed() must always return true
	 * when called on the same object.
	 * 
	 * The default implementation just marks this object as closed.
	 */
	public function close() {
		$this->closed = true;
	}

	/**
	 * Connects this gptransport to its peer, that is, the remote instance
	 * of GraphServ resp. graphCore. The information required to connect is
	 * typically provided to the constructor of the respective subclass.
	 */
	public abstract function connect();

	/**
	 * Sends a string to the peer. This is the a operation of the line based
	 * communication protocol.
	 */
    public abstract function send( $s );

	/**
	 * Receives a string from the peer. This is the a operation of the line based
	 * communication protocol.
	 */ 
    public abstract function receive( );
	
	/**
	 * Returns true when (and after) the ond of the data stream coming from the peer
	 * has been detected.
	 */
    public abstract function eof( );
    
    /**
     * creates an instance of gpDataSource for reading data from the current
     * position in the data stream coming from the peer.
     */
   	public abstract function make_source();

    /**
     * creates an instance of gpDataSink for writing data to the
     * data stream going to the peer.
     */
	public abstract function make_sink();

	/**
	 * Attempts to check if the peer is still responding. 
	 * 
	 * The default implementation does nothing.
	 */
	public function checkPeer() {
		// noop
	} 
	
	/**
	 * Sets the debug mode on this transport object. When debugging is enabled,
	 * details about all data send or received is deumpted to stdout. 
	*/
	public function setDebug($debug) {
		$this->debug = $debug;
	} 
	
}

/**
 * Base class for file handle based implementations of gpTransport.
 */
abstract class gpPipeTransport extends gpTransport {
	protected $hout = null;
	protected $hin = null;

	/**
	 * Utility function for sending data to a file handle. This is essentially
	 * a wrapper around fwrite, which makes sure that $s is written in its entirety.
	 * After $s was written out using fwrite, fflush is called to commit all
	 * data to the peer.
	 * 
	 * @param resource $hout the file handle to write to
	 * @param string $s the data to write
	 * 
	 * @throws gpProtocolException if writing fails.
	 */
    public static function send_to( $hout, $s ) {
		$len = strlen($s);
		
		for ($written = 0; $written < $len; $written += $c) {
			$c = fwrite($hout, substr($s, $written), $len - $written);
			
			if ($c === false) { // doc sais fwrite returns false on errors
				throw new gpProtocolException("failed to send data to peer, broken pipe! (fwrite returned false)");
			}

			if ($c === 0) { // experience sais fwrite returns 0 on errors
				throw new gpProtocolException("failed to send data to peer, broken pipe! (fwrite returned 0)");
			}
		}
		
		fflush( $hout );
		
		return $written;
	}
	
	/**
	 * Sends the given data string to the peer by writing it to the
	 * output file handle created by the connect() method. 
	 * Uses gpPipeTransport::send_to() to send the data.
	 */
    public function send( $s ) {
		return gpPipeTransport::send_to( $this->hout, $s );
	}
	
	/**
	 * Receives a string of data from the peer by reading a line from the 
	 * input file handle created by the connect() method. 
	 * Uses fgets to send the data. 
	 * 
	 * @todo: remove hardcoded limit of 1024 bytes per line!
	 */
	public function receive() {
		$re = fgets( $this->hin, 1024 ); //FIXME: what if response is too long?
		return $re;
	}

	/**
	 * Sets a read timeout on input file handle created by the connect() method.
	 */
	public function setTimeout( $seconds ) {
		stream_set_timeout($this->hin, $seconds);
	}

	/**
	 * Determines whether the input file handle created by the connect() method
	 * has reached the end of file (by using feof). This shoiuld be used to determine
	 * whether the communication with the peer was finished.
	 */
	public function eof() {
		return feof( $this->hin );
	}
	
	/**
	 * Returns a new instance of gpPipeSource that reads from the 
	 * input file handle created by the connect method().
	 */
	public function make_source() {
		return new gpPipeSource( $this->hin );
	}

	/**
	 * Returns a new instance of gpPipeSink that writes to the 
	 * output file handle created by the connect method().
	 */
	public function make_sink() {
		return new gpPipeSink( $this->hout );
	}
}

/**
 * A transport implementation for communicating with a remote instance
 * of GraphServ over TCP.
 */
class gpClientTransport extends gpPipeTransport {
	var $host;
	var $port;
	var $graphname;
	var $socket = false;

	/**
	 * Initializes a new instance of gpClientTransport with the information
	 * needed to connect to the GraphServ server instance.
	 * 
	 * @param string $host (default: 'localhost') the host the GraphServ process is located on.
	 * @param int $port (default: GP_PORT) the TCP port the GraphServ process is listening on.
	 */
	public function __construct( $host = false, $port = false ) {
		if ( !$host ) $host = 'localhost';
		if ( !$port ) $port = GP_PORT;

		$this->port = $port;
		$this->host = $host;
	}
	
	/**
	 * Connects to a remote instance of GraphServ using the host and port provided top the constructor.
	 * If the connection could be established, opens the graph specified to the constructor.
	 * 
	 * @throws gpProtocolException if the connection failed or another communication error ocurred.
	 */
	public function connect() {
		$this->socket = @fsockopen($this->host, $this->port, $errno, $errstr); //XXX: configure timeout?
		
		if ( !$this->socket ) throw new gpProtocolException( "failed to connect to " . $this->host . ":" . $this->port . ': ' . $errno . ' ' . $errstr );
		
		$this->hin = $this->socket;
		$this->hout = $this->socket;
		
		return true;
	}
	
	/**
	 * Closes the transport by disconnecting the TCP socket to the remote GraphServ instance
	 * (using fclose). Subsequent calls to close() have no further effect.
	 * 
	 * @throws gpProtocolException if the connection failed or another communication error ocurred.
	 */
	public function close() {
		if ( !$this->socket ) return false;
		
		@fclose( $this->socket );

		$this->closed = true;
	}
}

/**
 * A transport implementation for communicating with a GraphCore instance
 * running in a local child process (i.e. as a slave to the current PHP script).
 */
class gpSlaveTransport extends gpPipeTransport {
	var $process;
	var $command;

	/**
	 * Initializes a new instance of gpSlaveTransport with the information
	 * needed to launch a slave instance of GraphCore.
	 * 
	 * @param mixed $command the command line to start GraphCore. May be given as a
	 *        string or as an array. If given as a string, all parameters must be duely
	 *        escaped. If given as an array, $command[0] must be the path to the
	 *        GraphCore executable. See gpSlavetransport::makeCommand() for more details.
	 * @param string $cwd (default: null) the working dir to run the slave process in. 
	 *        Defaults to the current working directory.
	 * @param int $env (default: null) the environment variables to pass to the 
	 *        slave process. Defaults to inheriting the PHP script's environment.
	 */
	public function __construct( $command, $cwd = null, $env = null ) {
		$this->command = $command;
		
		$this->cwd = $cwd;
		$this->env = $env;
	}


	/**
	 * Utility function for creating a command line for executing a program
	 * as a child process. The first part of the command is the executable, 
	 * any following parts are passed as arguments to the executable.
	 * 
	 * @param $command the command, including the executable and any parameters.
	 *        May be given as a string or as an array. 
	 *        If given as a string, all parameters must be duely
	 *        escaped. If given as an array, $command[0] must be the path to an
	 * 		  executable.
	 * 
	 * @throws gpClientException if the command did not point to a readable,
	 *         executable file.
	 */
	public static function makeCommand( $command ) {
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
			if ( !file_exists( $path) ) throw new gpClientException('file does not exist: ' . $path);
			if ( !is_readable( $path) ) throw new gpClientException('file does not readable: ' . $path);
			if ( !is_executable( $path) ) throw new gpClientException('file does not executable: ' . $path);
		}
		
		return $cmd;
	}
	
	/**
	 * Connects to the slave instance of GraphCore launched using the command 
	 * provided to the constructor. proc_open() is used to launch the child process.
	 * 
	 * @throws gpClientException if the command executable could not be found.
	 * @throws gpProtocolException if the child process could not be launched.
	 * 
	 * @todo handle output to stderr!
	 * @todo get rid of the "wait 1/10 of a second and check" hack
	 */	
	public function connect() {
		$cmd = gpSlaveTransport::makeCommand( $this->command );
		
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
		
		return true;
	}
	
	/**
	 * Closes the transport by terminating the slave process using proc_close().
	 */
	public function close() {
		if ( !$this->process ) return false;
		
		@proc_close( $this->process );

		$this->process = false;
		$this->closed = true;
	}
	
	/**
	 * Checks if the slave process is still alive using proc_get_status().
	 * 
	 * @throws gpProtocolException if the slave process is dead.
	 */
	public function checkPeer() {
		$status = proc_get_status( $this->process );
		
		$this->trace(__METHOD__, "status", $status );

		if ( !$status['running'] ) throw new gpProtocolException('slave process is not running! exit code ' . $status['exitcode'] ); 
	} 
	
	
}

/**
 * This class represents an active connection to a graph. It can be seen as the local
 * interface to the graph that allows the graph to be queried and manipulated,
 * using the command set specified for GraphCore and GraphServ. The communication with
 * the peer process that manages the actual graph (a slave GraphCore instance or
 * a remote GraphCore server) is performed by an instance of the apprpriate subclass of
 * gpTransport.
 * 
 * Instances of gpConnection that use the appropriate transport can be created conveniently
 * using the static factory methods called new_xxx_connection.
 * 
 * Besides some methods for managing the connection and some utility functions,
 * gpConnection exposes the GraphCore and GraphServ command sets. The commands are
 * exposed as "virtual" methods: they are not implemented explicitely,
 * instead, the __call() method is used to map method calls to GraphCore commands.
 * No local checks are performed on the command call, so it's up to the peer to
 * decide which commands exist. Note that this means that a gpConnection actually exposes
 * more commands if it is connected to a GraphServ instance (simply because the peer
 * then supports more commands).
 * 
 * The mapping of method calls to commands is performed as follows:
 * 
 * * underscores are converted to dashes: the method add_arcs corresponds to the 
 *   add-arcs command in GraphCore. 
 * 
 * * any int or string parameters passed to the method are passed on to the
 *   command call, in the order they were specified.
 * 
 * * parameters that are instances of gpDataSource will be used to pass a data set
 *   to the command. That is, rows from the data source are passed to the command as
 *   input.
 * 
 * * parameters that are instances of gpDataSink will be used to handle any data
 *   the command outputs. That is, rows from the command's output data set
 *   will be passed to the data sink, one by one.
 * 
 * * parameters that are arrays are wrapped in a new instance of gpArraySource and used 
 *   as input for the command, as described above. This is convenient for passing 
 *   data directly to the command.
 * 
 * * parameters given as null or false are ignored.
 * 
 * * other types of arguments trigger a gpUsageException
 * 
 * A command called in this way, using its "plain" method counterpart, always
 * returns the status string from the peer's response upon successful execution.
 * The status may be "OK" or "NONE". Any failure on the server side triggers a
 * gpProcessorException. Any output of the command is passed to the gpDataSink 
 * that was provided as a parameter (or ignored if no sink was provided).
 * 
 * However, modifiers can be attached to the method name to cause the command's
 * outcome to be treated differently:
 * 
 * * if the method name is prefixed with "try_", no gpProcessorException are thrown.
 *   Instead, errors reported by the peer cause the method to return false.
 *   The cause of the error may be examined using the getStatus() and getStatusMessage()
 *   methods. Not that other exceptions like gpProtocollException or gpUsageException
 *   are still thrown as usual.
 * 
 * * if the method name is prefixed with "capture_", the command's output is collected
 *   and returned as an array of arrays, representing the rows of data. If the command
 *   fails, a gpProcessorException is raised, as usual (or, if try_ is also specified, 
 *   the method returns false).
 * 
 * * if the method name is suffixed with _map AND prefixed with "capture_", 
 *   the command's output is collected and returned as an associative array. 
 *   This is especially useful for commands like "stats" that provide values 
 *   for set of well known properties.
 *   To build the associative array, rows from the output are interpreted as 
 *   a key-value pairs. If the _map suffix is used without the capture_ prefix, 
 *   a gpUsageException is raised.
 * 
 * * if the method name is suffixed with _value, the command is expected
 *   to return a value in the status line, prefixed by either "OK." or "VALUE:". 
 *   The value is returned. If the command does not provide a VALUE or OK status,
 *   an exception is raised.
 * 
 * Modifiers can also be combined. For instance, try_capture_stats_map would
 * return GraphCore stats as an associative array, or null of the call failed.
 * 
 * Additional modifiers or extra virtual methods can be added by subclasses
 * by overriding the __call() method or by registering handlers with the
 * addCallHandler() or addExecHandler() methods.
 */
class gpConnection {
	
	/**
	 * The transport used to communicate with the peer that manages the actual graph.
	 */
	protected $transport = null;
	
	/**
	 * If true, the protocol session is "out of step" and no further commands can
	 * be processed.
	 */
	protected $tainted = false;
	
	/**
	 * The status string returned by the last command call.
	 */
	protected $status = null;
	
	/**
	 * The status message returned by the last command call.
	 */
	protected $statusMessage = null;
	
	/**
	 * The response from the last command call, including the status string and
	 * status message.
	 */
	protected $response = null;
	
	/**
	 * call handlers, see addCallHandler()
	 */
	protected $call_handlers = array();
	
	/**
	 * Exec handlers, see addExecHandler().
	 */
	protected $exec_handlers = array();
	
	/**
	 * If peer-side input and output redirection should be allowed. For security reasons,
	 * and to avoid confusion, i/o redirection is disabled per default.
	 */
	public $allowPipes = false;
	
	/**
	 * Whether arguments should be restricted to alphanumeric strings.
	 * Enabled by default.
	 */
	public $strictArguments = true;

	/**
	 * Debug mode enables lots of output to stdout.
	 */
	public $debug = false;
	
	/**
	 * Initializes a new connection with the given instance of gpTransport.
	 * 
	 * Note: Instances of gpConnection that use the appropriate transport can be created conveniently
	 * using the static factory methods called new_xxx_connection.
	 * 
	 * @param gpTransport $transport the transport object
	 * @param string $graphname (optional) the graph to connect to when connect() is called.
	 */
	public function __construct( gpTransport $transport, $graphname = null ) {
		$this->transport = $transport;
		$this->graphname = $graphname;
	}

	/**
	 * Connects to the peer. 
	 * 
	 * For connecting, this method relies solely on the
	 * transport instance, which in turn uses the information passed to its 
	 * constructor to establish the connection.
	 * 
	 * After connecting, this method calls checkProtocolVersion() to make sure
	 * the peer speaks the correct protocol version. If not, a gpProtocolException
	 * is raised.
	 * 
	 * If a graphname was supplied to the constructor, this function will
	 * call $this->use_graph($this->graphname) to connect to the given graph.
	 */
	public function connect() {
		$this->transport->connect();
		$this->checkProtocolVersion();

		if ( $this->graphname ) {
			try {
				$this->use_graph($this->graphname);
			} catch ( gpException $e ) {
				$this->close();
				throw $e;
			}
		}
	}
	
	/**
	 * Registers a call handler. The handler will be called before __call
	 * interprets a method call as a GraphCore command, and can be used to
	 * add support for additional virtual methods or extra modifiers.
	 * 
	 * The handler must be a callable that accepts the following parameters:
	 * 
	 * * $connection this gpConnection instance
	 * * &$cmd a reference to the command name, as a string, with the try_,
	 *         capture_, _map or _value modifiers removed.  
	 * * &$args a reference to the argument array, unprocessed, as passed to
	 *          the method.
	 * * &$source a reference to a gpDatSource (or null), may be altered to 
	 *            change the command's input.
	 * * &$sink a reference to a gpDatSink (or null), may be altered to 
	 *            change the output handlking for the command.
	 * * &$capture a reference to the capture flag. If true, output will be 
	 *             captured and returned as an array.
	 * * &$result the result to return from the method call, used only if
	 *            the handler returns false.
	 * 
	 * If the handler returns false, the value of $result will be returned
	 * from __call and no further action is taken.
	 */
	public function addCallHandler( $handler ) { //$handler($connection, &$cmd, &$args, &$source, &$sink, &$capture, &$result)
		$this->call_handlers[] = $handler;
	}
	
	/**
	 * Registers a call handler. The handler will be called before __call
	 * passes a command to the exec() method, and can thus be used to
	 * add support for additional "artificial" commands that use the same parameter
	 * handling as is used for "real" GraphCore commands.
	 * 
	 * The handler must be a callable that accepts the following parameters:
	 * 
	 * * $connection this gpConnection instance
	 * * &$command a refference to the command, as an array. The first field is the command name,
	 *             the remaining fields contain the parameters for the command.
	 * * &$source a reference to a gpDatSource (or null), may be altered to 
	 *            change the command's input.
	 * * &$sink a reference to a gpDatSink (or null), may be altered to 
	 *            change the output handlking for the command.
	 * * &$status the commands return status, used of the handler returns false.
	 * 
	 * If the handler returns false, the value of $status will be used as the
	 * command's result, and no command will be sent to the peer. The value
	 * of $status is treated the same way the status returned from the peer is:
	 * e.g. a gpProcessorException is thrown if the status is "FAILED", etc.
	 * Also, modifiers like capture_ are applied to the output in the same way
	 * as they are for "normal" commands.
	 */
	public function addExecHandler( $handler ) { //$handler($connection, &$command, &$source, &$sink, &$has_output, &$status)
		$this->exec_handlers[] = $handler;
	}
	
	/**
	 * Returns the status string that resulted from the last command call. The status string is 
	 * 'OK' or 'NONE' for successfull calls, or 'FAILED', 'ERROR' or 'DENIED' for unsuccessful
	 * calls. Refer to the GraphCore and GraphServ documentation for details.
	 */
	public function getStatus() {
		return $this->status;
	}
	
	/**
	 * Returns true if close() was called on this connection, or it was closed for
	 *  some other reason. No commands can be called on a closed connection.
	 */
	public function isClosed() {
		return $this->transport->isClosed();
	}
	
	/**
	 * Returns the status message that resulted from the last command call. The status message
	 * is the informative message that follows the status string in the response from a command
	 * call. It may be useful for human eyes, but should not be processed programmatically.
	 */
	public function getStatusMessage() {
		return $this->statusMessage;
	}
	
	/**
	 * Returns the response that the last command call evoked, consisting of the status string
	 * and the status message.
	 */
	public function getResponse() {
		return $this->response;
	}
	
	/**
	 * Utility method for printing messages to stdout when debug mode is enabled.
	 */
	protected function trace( $context, $msg, $obj = 'nothing878423really' ) {
		if ( $this->debug ) {
			if ( $obj !== 'nothing878423really' ) {
				$msg .= ': ' . preg_replace( '/\s+/', ' ', var_export($obj, true) );
			}
			
			print "[gpClient] $context: $msg\n";
		}
	}
	
	/**
	 * Attempts to check if the peer is still alive.
	 */
	public function checkPeer() {
		$this->transport->checkPeer();
	} 
	
	/**
	 * Enabled or disables the debug mode. In debug mode, tons of diagnostic
	 * information are written to stdout.
	 * 
	 * @param bool $debug 
	 */
	public function setDebug($debug) {
		$this->debug = $debug;
		$this->transport->setDebug($debug);
	} 
	
	/**
	 * Returns the protocol version reported by the peer.
	 */
	public function getProtocolVersion() {
		if ( empty($this->protocol_version) ) { 
			$this->protocol_version();
			$this->protocol_version = trim($this->statusMessage);
		}
		
		return $this->protocol_version;
	} 
	
	/**
	 * Raises a gpProtocolException if the protocol version reported by the peer is
	 * not compatible with GP_MIN_PROTOCOL_VERSION and GP_MAX_PROTOCOL_VERSION.
	 */
	public function checkProtocolVersion() {
		$version = (float)$this->getProtocolVersion();
		
		if ( $version < GP_MIN_PROTOCOL_VERSION ) { 
			throw new gpProtocolException( "Bad protocol version: expected at least " . GP_MIN_PROTOCOL_VERSION . ", but peer uses " . $version );
		}

		if ( $version > GP_MAX_PROTOCOL_VERSION ) {
			throw new gpProtocolException( "Bad protocol version: expected at most " . GP_MAX_PROTOCOL_VERSION . ", but peer uses " . $version );
		}
	} 
	
	/**
	 * Attempts to check if the peer is still responding.
	 */
	public function ping() {
		$re = $this->protocol_version();
		$this->trace(__METHOD__, $re);
		
		return $re;
	}
	
	/**
	 * implementation of the magic __call() method that intercepts calls to
	 * undeclared methods and mapps them to calls to graph commands on the peer.
	 * Refer to the class level documentation of gpConnection for details.
	 * 
	 * @param string name the method name
	 * @param array arguments the arguments passed to the method
	 */
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
		
		if ( preg_match( '/-map$/', $cmd ) ) {
			if (!$capture) throw new gpUsageException( "using the _map suffix without the capture_ prefix is meaningless" );
			$cmd = substr( $cmd, 0, strlen($cmd) -4 );
			$map = true;
		} else { 		
			$map = false;
		}
		
		if ( preg_match( '/-value$/', $cmd ) ) { 
			if ($capture) throw new gpUsageException( "using the _value suffix together with the capture_ prefix is meaningless" );
			
			$cmd = substr( $cmd, 0, strlen($cmd) -6 );
			$val = true;
		} else { 		
			$val = false;
		}
		
		$result = null;
		foreach ( $this->call_handlers as $handler ) {
			$continue = call_user_func_array( $handler, array( $this, &$cmd, &$arguments, &$source, &$sink, &$capture, &$result ) );
			if ( $continue === false ) return $result;
		}
		
		$command = array( $cmd );
		
		foreach ( $arguments as $arg ) {
			if ( is_array( $arg ) ) {
				$source = new gpArraySource( $arg );
			} else if ( is_object( $arg ) ) {
				if ( $arg instanceof gpDataSource ) $source = $arg;
				else if ( $arg instanceof gpDataSink ) $sink = $arg;
				else throw new gpUsageException( "arguments must be primitive or a gpDataSource or gpDataSink. Found " . get_class($arg) );
			} else if ( $arg === null || $arg === false ) {
				continue;
			} else if ( is_string($arg) || is_int($arg) ) {
				$command[] = $arg;
			} else {
				throw new gpUsageException( "arguments must be objects, strings or integers. Found " . type($arg) );
			}
		}
		
		try {
			$do_exec = true;
			$has_output = null;
			
			foreach ( $this->exec_handlers as $handler ) {
				$continue = call_user_func_array( $handler, array( $this, &$command, &$source, &$sink, &$has_output, &$status) );
				
				if ( $continue === false ) {
					$do_exec = false;
					break;
				}
			}
		
			if ( $do_exec ) {
				$func = str_replace('-', '_', $command[0]. '_impl');
				if ( method_exists( $this, $func ) ) {
					$args = array_slice( $command, 1 );
					$args[] = $source;
					$args[] = $sink;
					$args[] = &$has_output;
					
					$status = call_user_func_array( array($this, $func), $args );
				} else {
					$status = $this->exec( $command, $source, $sink, $has_output );
				}
			}
		} catch ( gpProcessorException $e ) { //XXX: catch more exceptions? ClientException? Protocolexception?
			if ( !$try ) throw $e;
			else return false;
		}

		//note: call modifiers like "_capture" change the return type!
		if ( $capture ) {
			if ( $status == 'OK' || $status == 'VALUE' ) { 
				if ( $has_output ) {
					if ($map) return $sink->getMap();
					else return $sink->getData();
				} else {
					return true;
				}
			}
			else if ( $status == 'NONE' ) return null;
			else return false;
		} else {
			if ( $result ) $status = $result; // from handler
			
			if ( $val ) { 
				if ( $status == "VALUE" || $status == "OK" ) {
					return $this->statusMessage; #XXX: not so pretty
				} else {
					throw new gpUsageException( "Can't apply _value modifier: command " . $command . " did not return a VALUE or OK status, but this: " . $status );
				}
			}
			
			return $status;
		}
    }
    
	/**
	 * Applies a command to the graph, that is, runs the command on the peer.
	 * 
	 * Note: this method implements the protocol used to interact with the peers,
	 * based upon the line-by-line communication provided by the transport 
	 * instance. Interaction with the peer is stateless between calls to this
	 * function (except of course for the contents of the graph itself).
	 * 
	 * @param mixed $command the command, as a single string or as an array containing
	 *              the command name and any arguments.
	 * @param gpDataSource $source the data source to take the commands input from (or null)
	 * @param gpDataSink $sink the data sink to pass the commands output to (or null)
	 * @param bool &$has_output a reference parameter that gets set to true if the command
	 *        generated output, and to false if it didn't.
	 * 
	 * @return string containing the status string returned by the command
	 * 
	 * @throws gpProtocolException if a communication error ocurred while talking to the peer
	 * @throws gpProcessorException if the peer reported an error
	 * @throws gpUsageException if $command does not conform to the rules for commands. Note that
	 *         $this->strictArguments and $this->allowPipes influence which commands are allowed.
	 */
	public function exec( $command, gpDataSource $source = null, gpDataSink $sink = null, &$has_output = null ) {
		$this->trace(__METHOD__, "BEGIN");
		
		if ( $this->tainted ) {
			throw new gpProtocolException("connection tainted by previous error!");
		}
		
		if ( $this->isClosed() ) {
			throw new gpProtocolException("connection already closed!");
		}
		
		if ( $this->transport->eof() ) { // closed by peer
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
			
			if ( $c == "set-meta" || $c == "authorize" ) { #XXX: ugly hack for wellknown commands
				$strictArgs = false;
			}
			
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
		$this->transport->send( $command . GP_LINEBREAK ); 
		
		$this->trace(__METHOD__, "source", $source == null ? null : get_class($source));
		
		if ( $source ) {
			$this->copyFromSource( $source );
		}
		
		$re = $this->transport->receive( ); 
		$this->trace(__METHOD__, "<<< ", $re);
		
		if ( $re === '' || $re === false || $re === null ) {
			$this->tainted = true;
			$this->status = null;
			$this->statusMessage = null;
			$this->response = null;
			
			$this->trace(__METHOD__, "peer did not respond! Got value " . var_export($re, true));
			$this->transport->checkPeer();
			
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
		
		if ( $this->status != 'OK' && $this->status != 'NONE' && $this->status != 'VALUE' ) {
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
		
		if ( $this->transport->eof() ) { // closed by peer
			$this->trace(__METHOD__, "connection closed by peer, closing our side too.");
			$this->close();
		}
		
		return $this->status;
	}

	/**
	 * Implements a "fake" command traverse-successors-without which returns all decendants of onw nodes
	 * minus the descendants of some other nodes. This is a convenience function for a common case that
	 * could otherwise only be covered by implementing the set operation in php, or by using exec().
	 * 
	 * This method should not be called directly. Instead, use the virtual method traverse_successors_without
	 * in the same way as normal commands are called. This include support for modifiers and flexible
	 * handling of method parameters.
	 */
	public function traverse_successors_without_impl( $id, $depth, $without, $without_depth, $source, $sink, &$has_output = null ) {
		if ( !$without_depth ) $without_depth = $depth;
		return $this->exec( "traverse-successors $id $depth &&! traverse-successors $without $without_depth", $source, $sink, $has_output );
	}
		
	/**
	 * Checks if the given name is a valid command name. Command names consist of
	 * a letter followed by any number of letters, numbers, or dashes.
	 */
	public static function isValidCommandName( $name ) {
		return preg_match('/^[a-zA-Z_][-\w]*$/', $name);
	}
	
	/**
	 * Checks if the given string passes some sanity checks. The command string must
	 * start with a valid command, and it must not contain any non-printable or
	 * non-ascii characters.
	 */
	public static function isValidCommandString( $command ) {
		if ( !preg_match('/^[a-zA-Z_][-\w]*\s*(:?\s*$|[\s!&]+\w|[|<>#])/', $command) ) return false; // must start with a valid command
		
		return !preg_match('/[\0-\x1F\x80-\xFF]/', $command);
	}
	
	/**
	 * Checks if the given string is a valid argument. If $strict is set, 
	 * it checks of $arg consists of an alphanumeric character followed by
	 * any number of alphanumerics, colons or dashes. If $strict is not set,
	 * this just checks that $arg doesn't contain any non-printable or
	 * non-ascii characters.
	 * 
	 * @param string $arg the argument to check
	 * @param bool $strict whether to perform a strict check (default: true).
	 */
	public static function isValidCommandArgument( $arg, $strict = true ) {
		if ( $arg === '' || $arg === false || $arg === null ) return false;

		if ( $strict ) return preg_match('/^\w[-\w]*$/', $arg);
		else return !preg_match('/[\s\0-\x1F\x80-\xFF|<>!&#]/', $arg); //space, low chars, high chars, and operators. 
	}
	
	/**
	 * Converts a line from a data set into an array. If $s is empty,
	 * this method returns false. if $s starts with "#", it's considered
	 * to consist of a single string field. Otherwise, the string is split
	 * on ocurrances of TAB, semikolon or comma. Numeric field values are
	 * converted to int, other feelds remain strings.
	 * 
	 * @param string $s the row from the data set, as a string
	 * 
	 * @return array containing the fields in $s, or false if $s is empty.
	 */
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

	/**
	 * Joins an array into a data set row represented as a string.
	 * Values in $row are joined together using commas as separators.
	 * 
	 * @param array $row the data row
	 * 
	 * @return string containing the fields from $row
	 */
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
	
	/**
	 * Copies all data from the given source into the current command stream,
	 * that is, passes them to the client line by line. 
	 * 
	 * Note that this must only be called after passing a command line terminated by ":"
	 * to the peer, so the peer expects a data set.
	 * 
	 * This is implemented by calling the transport's make_sink() method
	 * to create a sink for writing to the command stream, and then using the copy()
	 * method to transfer the data.
	 * 
	 * Note that $source is not automatically closed by this method.
	 */
	protected function copyFromSource( gpDataSource $source ) {
		$sink = $this->transport->make_sink();
		
		$this->trace(__METHOD__, "source", get_class($source));
		
		$this->copy( $source, $sink, ' > ' );

		// $source->close(); // to close or not to close...

		$this->transport->send( GP_LINEBREAK ); //XXX: flush again??

		$this->trace(__METHOD__, "copy complete.");

		/*
		while ( $row = $source->nextRow() ) {
			$s = gpConnection::joinRow( $row );
			
			fputs($this->hout, $s . GP_LINEBREAK);
		}

		fputs($this->hout, GP_LINEBREAK); // blank line
		*/
	}

	/**
	 * Copies all data from the command response into the given sink,
	 * that is, receives data from the peer line by line. 
	 * 
	 * Note that this must only be called after the peer sent a response line that
	 * endes with ":", so we know the peer is waiting to send a data set.
	 * 
	 * This is implemented by calling the transport's make_source() method
	 * to create a source for reading from the command stream, and then using the copy()
	 * method to transfer the data.
	 * 
	 * Note that $sink is flushed but not closed before this method returns.
	 */
	protected function copyToSink( gpDataSink $sink = null ) {
		$source = $this->transport->make_source();
		
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
	
	/**
	 * Utility method for transferring all rows from a data source to a data sink.
	 * If $sink is null, all rows are read from the source and then discarded.
	 * 
	 * Before returning, the sink is flushed to commit any pending data.
	 */
	public function copy( gpDataSource $source, gpDataSink $sink = null, $indicator = '<>' ) {
		while ( $row = $source->nextRow() ) {
			if ( $sink) {
				$this->trace(__METHOD__, $indicator, $row);
				$sink->putRow( $row );
			} else {
				$this->trace(__METHOD__, "#", $row);
			}
		}
		
		if ( $sink ) $sink->flush();
	}

	/**
	 * Closes this connection by closing the underlying transport.
	 */
	public function close() {
		$this->transport->close();
	}
	 
	/**
	 * Creates a new connection for accessing a remote graph managed by a GraphServ service.
	 * Returns a gpConnection that uses a gpClientTransport to talk to the remote graph.
	 * 
	 * @param string $graphname the name of the graph to connect to
	 * @param string $host (default: 'localhost') the host the GraphServ process is located on.
	 * @param int $port (default: GP_PORT) the TCP port the GraphServ process is listening on.
	 */
	public static function new_client_connection( $graphname, $host = false, $port = false ) {
		return new gpConnection( new gpClientTransport($host, $port), $graphname );
	}

	/**
	 * Creates a new connection for accessing a graph managed by a slave GraphCore process.
	 * Returns a gpConnection that uses a gpSlaveTransport to talk to the local graph.
	 * 
	 * @param mixed $command the command line to start GraphCore. May be given as a
	 *        string or as an array. If given as a string, all parameters must be duely
	 *        escaped. If given as an array, $command[0] must be the path to the
	 *        GraphCore executable. See gpSlavetransport::makeCommand() for more details.
	 * @param string $cwd (default: null) the working dir to run the slave process in. 
	 *        Defaults to the current working directory.
	 * @param int $env (default: null) the environment variables to pass to the 
	 *        slave process. Defaults to inheriting the PHP script's environment.
	 */
	public static function new_slave_connection( $command, $cwd = null, $env = null ) {
		return new gpConnection( new gpSlaveTransport($command, $cwd, $env) );
	}
}

/**
 * Extracts a column from a tabular structure
 * 
 * @param array $a an array of equal-sized arrays, representing a table as a list of rows.
 * @param mixed $col the column key (usually an int or string) of the column to extract
 * 
 * @return an array consisting of the values of column $col from each row in $a
 */
function array_column($a, $col) {
	$column = array();
	
	foreach ( $a as $k => $x ) {
		$column[$k] = $x[$col];
	}
	
	return $column;
}

/**
 * Converts a list of key value pairs to an associative array (aka a map).
 * 
 * @param array $pairs an array of key value paris, representing a map as a list of tuples.
 * @param mixed $key_col the column that contains the key (default: 0)
 * @param mixed $value_col the column that contains the value (default: 1)
 * 
 * @return an associative array built from the key value pairs in $pairs.
 */
function pairs2map( $pairs, $key_col = 0, $value_col = 1 ) {
	$map = array();
	
	foreach ( $pairs as $p ) {
		$k = $p[$key_col];
		$map[ $k ] = $p[$value_col];
	}
	
	return $map;
}
	
	
