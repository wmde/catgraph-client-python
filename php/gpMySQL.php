<?php
require_once(dirname(__FILE__) . "/gpClient.php");

class gpMySQLSource extends gpDataSource {
	var $glue;
	var $result;
	var $table;
	
	public function __construct( gpMySQLGlue $glue, $result, $table) {
		$this->glue = $glue;
		$this->result = $result;
		$this->table = $table;
		
		$this->field1 = $table->get_field1(true);
		$this->field2 = $table->get_field2(true);
	}

	public function nextRow() {
		$raw = $this->glue->mysql_fetch_assoc( $this->result );
		
		if ( !$raw ) return null;
		
		$row = array();
		$row[] = $raw[ $this->field1 ];
		
		if ( $this->field2 ) {
			$row[] = $raw[ $this->field2 ];
		}
				
		return $row;
	}
	
	public function close( ) {
		$this->glue->mysql_free_result( $this->result ); 
	}
}

class gpMySQLTable {
	private $name;
	private $field1;
	private $field2;
	
	function __construct($name, $field1, $field2 = null) {
		$this->name = $name;
		$this->field1 = $field1;
		$this->field2 = $field2;
	}
	
	function get_name() {
		return $this->name;
	}  

	static function strip_qualifier( $n ) {
		return preg_replace('/^.*\./', '', $n);
	}
	
	function get_field1( $basename_only = false ) {
		if ( $basename_only ) return gpMySQLTable::strip_qualifier( $this->field1 );
		else return $this->field1;
	}
	
	function get_field2( $basename_only = false ) {
		if ( $basename_only ) return gpMySQLTable::strip_qualifier( $this->field2 );
		else return $this->field2;
	}

	function get_fields() {
		if ( $this->field2 ) return array( $this->field1, $this->field2 );
		else return array( $this->field1 );
	}  

	function get_field_list() {
		if ( $this->field2 ) return "{$this->field1}, {$this->field2}";
		else return $this->field1;
	}  

	function get_select() {
		return "SELECT " . $this->get_field_list() . " FROM " . $this->get_name();
	}  

	function get_insert( $ignore = false ) {
		$ig = $ignore ? "IGNORE" : "";
		return "INSERT $ig INTO " . $this->get_name() . " ( " . $this->get_field_list() . " ) ";
	}  

	function get_order_by() {
		return "ORDER BY " . $this->get_field_list();
	}  
}

class gpMySQLSelect extends gpMySQLTable {
	var $select;
	
	function __construct($select) {
		if ( preg_match('/^\s*select\s+(.*?)\s+from\s+([^ ]+)(?:\s+(.*))?/is', $select, $m) ) {
			$this->select = $select;
			
			$n = $m[2];
			$ff = preg_split('/\s*,\s*/', $m[1]);
			
			foreach ( $ff as $i => $f ) {
				$f = preg_replace('/^.*\s+AS\s+/i', '', $f); // use alias if defined
				$ff[$i] = $f;
			}
			
			parent::__construct($n, $ff[0], @$ff[1]);
		} else {
			throw new gpUsageException("can't parse statement: " . $select);
		}
	}

	function get_select() {
		return $this->select;
	}  

	function get_insert( $ignore = false ) {
		throw new gpUsageEsxception("can't create insert statement for: " . $this->select);
	}  
}

abstract class gpMySQLInserter {
	var $glue;
	var $table;
	var $fields;

	function __construct ( gpMySQLGlue $glue, gpMySQLTable $table ) {
		$this->glue = $glue;
		$this->table = $table;
	}
	
	public abstract function insert( $values );

	public function flush() {
		//noop
	}
	
	public function close() {
		$this->flush();
	}
}

class gpMySQLSimpleInserter extends gpMySQLInserter {

	public function as_list( $values ) {
		return $this->glue->as_list( $values );
	}
	
	protected function insert_command( ) {
		return $this->table->get_insert();
	}
	
	public function insert( $values ) {
		$sql = $this->insert_command();
		$sql .= " VALUES ";
		$sql .= $this->as_list($values);
		
		$this->glue->mysql_query( $sql );
	}

}

class gpMySQLBufferedInserter extends gpMySQLSimpleInserter {

	function __construct ( gpMySQLGlue $glue, gpMySQLTable $table ) {
		parent::__construct( $glue, $table );
		$this->buffer = "";
	}

	public function insert( $values ) {
		$vlist = $this->as_list($values);
		$max = $this->glue->get_max_allowed_packet();

		if ( !empty($this->buffer) && ( strlen($this->buffer) + strlen($vlist) + 2 >= $max ) ) {
			$this->flush();
		}
		
		if ( empty($this->buffer) ) {
			$this->buffer = $this->insert_command();
			$this->buffer .= " VALUES ";
		} else {
			$this->buffer .= ", ";
		}
		
		$this->buffer .= $vlist;

		if ( strlen($this->buffer) >= $max ) {
			$this->flush();
		}
	}
	
	public function flush() {
		if ( !empty( $this->buffer ) ) {
			$this->glue->mysql_query( $this->buffer );
			$this->buffer = "";
		}
	}

}

class gpMySQLSink extends gpDataSink {
	
	public function __construct( gpMySQLInserter $inserter ) {
		$this->inserter = $inserter;
	}
	
	public function putRow( $row ) {
		$this->inserter->insert( $row );
	}
	
	public function flush( ) {
		$this->inserter->flush();
	}
	
	public function close( ) {
		parent::close();
		$this->inserter->close();
	}
	
	public function drop( ) {
		throw new gpUsageException("only temporary sinks can be dropped");
	}
}

class gpMySQLTempSink extends gpMySQLSink {
	var $glue;
	var $table;
	
	public function __construct( gpMySQLInserter $inserter, gpMySQLGlue $glue, gpMySQLTable $table ) {
		parent::__construct( $inserter );
		
		$this->glue = $glue;
		$this->table = $table;
	}
	
	public function drop( ) {
		$sql = "DROP TEMPORARY TABLE IF EXISTS " . $this->table->get_name(); 
		
		$ok = $this->glue->mysql_query( $sql );
		return $ok;
	}
	
	public function getTable() {
		return $this->table;
	}

	public function getTableName() {
		return $this->table;
	}
	
}


class gpMySQLGlue extends gpConnection {
	var $connection;
	
	function __construct( $transport ) {
		parent::__construct($transport);

		$h = array( $this, 'gp_mysql_call_handler' );
		$this->addCallHandler( $h );
	}
	
	function mysql_connect( $server = null, $username = null, $password = null, $new_link = false, $client_flags = 0 ) {
		$this->connection = @mysql_connect($server, $username, $password, $new_link, $client_flags);
		$errno = mysql_errno( );
			
		if ( $errno ) {
			throw new gpClientException( "Failed to connect! MySQL Error $errno: " . mysql_error() );
		}
		
		if ( !$this->connection ) {
			throw new gpClientException( "Failed to connect! (unknown error)" );
		}
		
		return true;
	}

	function set_mysql_connection( $connection ) {
		$this->connection = $connection;
	}
	
	function gp_mysql_call_handler($gp, &$cmd, &$args, &$source, &$sink, &$capture, &$result) {
		if ( preg_match( '/-(from|into)$/', $cmd, $m ) ) {
			$cmd = preg_replace('/-(from|into)?$/', '', $cmd);
			$action = $m[1];
			
			$c = count($args);
			if ( !$c ) {
				throw new gpUsageException("expected last argument to be a table spec; " . var_export($args, true));
			}
			
			$t = $args[$c-1];
			$args = array_slice($args, 0, $c-1);
			
			if ( is_string($t) ) {
				if ( preg_match('/^.*select\s+/i', $t) ) $t = new gpMySQLSelect($t);
				else $t = preg_split( '/\s+|\s*,\s*/', $t ); 
			}
			
			if ( is_array($t) ) $t = new gpMySQLTable( $t[0], $t[1], @$t[2] ); 
			if ( ! ($t instanceof gpMySQLTable) ) throw new gpUsageException("expected last argument to be a table spec; found " . get_class($t));
			
			if ( $action == 'into' ) {
				if ( !$t->get_name() || $t->get_name() == "?" ) $sink = $this->make_temp_sink( $t ); 
				else $sink = $this->make_sink( $t );
				
				$result = $sink; //XXX: quite useless, but consistent with -from
			} else {
				$source = $this->make_source( $t );
				
				$result = $source; //XXX: a bit confusing, and only useful for temp sinks
			}
		} 
		
		return true;
	}
	
	private function call_mysql( $name, $args ) {
		$rc = false;
		
		if ( method_exists( $this, $name ) ) {
			return call_user_func_array( array($this, $cmd), $args );
		}
		
		//see if there's a resource in $args
		foreach ( $args as $a ) {
			if ( is_resource( $a ) ) {
				$rc = true;
				break;
			}
		}
		
		//if there was no resource in $args, add the connection
		if ( !$rc ) {
			$args[] = $this->connection;
		}
		
		$res = call_user_func_array( $name, $args );

		if ( !$res ) {
			$errno = mysql_errno( $this->connection );
			
			if ( $errno ) {
				throw new gpClientException( "MySQL Error $errno: " . mysql_error() );
			}
		}

		return $res;
	}
	
	function __call( $name, $args ) {
		if ( preg_match('/^mysql_/', $name) ) {
			return $this->call_mysql($name, $args);
		} else {
			return parent::__call($name, $args);
		}
	}
	
	
	public function quote_string( $s ) {
		return "'" . mysql_real_escape_string( $s ) . "'";
	}
	
	public function as_list( $values ) {
		$sql = "(";

		$first = true;
		foreach ( $values as $v ) {
			if ( !$first ) $sql .= ",";
			else $first = false;
			
			if ( is_null($v) ) $sql.= "NULL";
			else if ( is_int($v) ) $sql.= $v;
			else if ( is_float($v) ) $sql.= $v;
			else if ( is_str($v) ) $sql.= $this->glue->quote_string($v); //TODO: charset...
			else throw new gpUsageException("bad value type: " . gettype($v));
		}
		
		$sql .= ")";
		
		return $sql;
	}
	
	private function next_id() {
		static $id = 1;
		
		$id++;
		return $id;
	} 
	
	public function make_temp_table( $spec ) {
		$table = $spec->get_name();
		
		if ( !$table || $table === '?' ) { 
			$table = "gp_temp_" . $this->next_id();
		}
		
		$sql = "CREATE TEMPORARY TABLE " . $table; 
		$sql .= "(";
		$sql .= $spec->get_field1() . " INT NOT NULL";
		if ($spec->get_field2()) $sql .= ", " . $spec->get_field2() . " INT NOT NULL";
		$sql .= ")";
		
		$this->mysql_query($sql);
		
		return new gpMySQLTable($table, $spec->get_field1(), $spec->get_field2());  
	}

	public function mysql_query_value( $sql ) {
		$res = $this->mysql_query( $sql );
		$a = $this->mysql_fetch_row( $res );
		$this->mysql_free_result( $res );
		
		if ( !$a ) return null;
		else return $a[0];
	}
	
	public function set_max_allowed_packet( $size ) {
		$this->max_allowed_packet = $size;
	}
	
	public function get_max_allowed_packet() {
		if ( empty( $this->max_allowed_packet ) ) {
			$this->max_allowed_packet = $this->mysql_query_value("select @@max_allowed_packet");
		}

		if ( empty( $this->max_allowed_packet ) ) {
			$this->max_allowed_packet = 16 * 1024 * 1024; //fall back to MySQL's default of 16MB
		}
		
		return $this->max_allowed_packet;
	}

	public function select_into( $query, gpDataSink $sink ) {
		if ( is_string($query) ) {
			$table = new gpMySQLSelect( $query );
			$sql = $query;
		} else {
			$table = $query;
			$sql = $src->get_select();
		}
		
		$res = $this->mysql_query( $sql );
		$src = new gpMySQLSource( $this, $res, $table );
		
		$c = $this->copy( $src, $sink, '+' );
		$src->close();
		
		return $c;
	}
	
	protected function new_inserter( gpMySQLTable $table ) {
		return new gpMySQLBufferedInserter( $this, $table );
	}
	
	public function make_temp_sink( gpMySQLTable $table ) {
		$table = $this->make_temp_table($table);
		
		$ins = $this->new_inserter($table);
		$sink = new gpMySQLTempSink( $ins, $this, $table );
		
		return $sink;
	}

	public function make_sink( gpMySQLTable $table ) {
		$inserter = $this->new_inserter($table);
		$sink = new gpMySQLSink( $inserter );
		
		return $sink;
	}

	public function make_source( $table, $big = false ) {
		$sql = $table->get_select();
		
		if ( !preg_match('/\s+ORDER\s+BY\s+/i', $sql) ) {
			$sql .= ' ' . $table->get_order_by();
		}
		
		if ($big) $res = $this->mysql_unbuffered_query($sql);
		else $res = $this->mysql_query($sql);
		
		$src = new gpMySQLSource( $this, $res, $table );
		return $src;
	}

	public function query_to_file( $query, $file, $remote = false ) {
		$r = $remote ? "" : "LOCAL"; //TESTME
		
		$query .= " INTO $r DATA OUTFILE "; //TESTME
		$query .= $this->quote_string($file);
		
		return $this->mysql_query($query);
	}

	public function insert_from_file( $table, $file, $remote = false ) {
		$r = $remote ? "" : "LOCAL"; //TESTME

		$query = "";
		$query .= " LOAD $r DATA INFILE "; //TESTME
		$query .= $this->quote_string($file);
		$query .= " INTO TABLE $table";
		
		return $this->mysql_query($query);
	}
	
	public function close() {
		$this->mysql_close();
		parent::close();
	}

	 
	public static function new_client_connection( $graphname, $host = false, $port = false ) {
		return new gpMySQLGlue( new gpClientTransport($graphname, $host, $port) );
	}

	public static function new_slave_connection( $command, $cwd = null, $env = null ) {
		return new gpMySQLGlue( new gpSlaveTransport($command, $cwd, $env) );
	}
	
	public function dump_query( $sql ) {
		print "\n*** $sql ***\n";
		
		$res = $this->mysql_query( $sql );
		if ( !$res ) return false; 
		
		return $this->dump_result( $res );
	}
	
	public function dump_result( $res ) {
		$keys = null;
		$c = 0;
		
		print "\n";
		while ( $row = $this->mysql_fetch_assoc( $res ) ) {
			if ( $keys === null ) {
				$keys = array_keys( $row );
				
				foreach ( $keys as $k ) {
					print "$k\t";
				}
			}
			
			foreach ( $row as $k => $v ) {
				print "$v\t";
			}
			
			print "\n";
			$c++;
		}
		
		print "-----------------------------\n";
		print "$c rows";
		
		return $c;
	}
}
