<?php

class gpMySQLSource extends gpDataSource {
	var $mysql;
	var $result;
	var $field1;
	var $field2;
	
	public function __construct( gpMySQL $mysql, $result, $field1, $field2 = null) {
		$this->mysql = $mysql;
		$this->result = $result;
		$this->field1 = $field1;
		$this->field2 = $field2;
	}

	public function nextRow() {
		$raw = $this->mysql->fetch_assoc( $this->result );
		
		if ( !$raw ) return null;
		 
		$row = array();
		$row[] = $raw[ $this->field1 ];
		
		if ( $this->field2 ) {
			$row[] = $raw[ $this->field2 ];
		}
				
		return $row;
	}
	
	public function close( ) {
		$this->mysql->free_result( $this->result ); 
	}
}

abstract class gpMySQLInserter {
	var $mysql;
	var $table;
	var $fields;

	function __construct ( gpMySQL $mysql, $table, $field1 = null, $field2 = null ) {
		$this->mysql = $mysql;
		$this->table = $table;
		
		$this->fields = array();
		if ($field1) $this->fields[] = $field1;
		if ($field2) $this->fields[] = $field2;
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

	protected function as_list( $values ) {
		$sql = "(";

		$first = true;
		foreach ( $values as $v ) {
			if ( !$first ) $sql .= ",";
			else $first = false;
			
			if ( is_null($v) ) $sql.= "NULL";
			else if ( is_int($v) ) $sql.= $v;
			else if ( is_float($v) ) $sql.= $v;
			else if ( is_str($v) ) $sql.= '"' . $this->mysql->real_escape_string($v) . '"'; //TODO: charset...
			else throw new gpUsageException("bad value type: " . gettype($v));
		}
		
		$sql .= ")";
		
		return $sql;
	}
	
	protected function insert_command( ) {
		$sql = "INSERT IGNORE INTO {$this->table} ";
		
		if ( $this->fields ) {
			$sql .= " ( ". implode(",", $this->fields) ." ) ";
		}
		
		return $sql;
	}
	
	public function insert( $values ) {
		$sql = $this->insert_command();
		$sql .= " VALUES ";
		$sql .= $this->as_list($values);
		
		$this->mysql->query( $sql );
	}

}

class gpMySQLSink extends gpDataSink {
	
	public function __construct( gpMySQLInserter $inserter ) {
		$this->inserter = $inserter;
	}
	
	public function putRow( $row ) {
		$this->inserter->insert( $row );
	}
	
	public function close( ) {
		$this->inserter->close();
	}
	
	public function drop( ) {
		throw new gpUsageException("only temporary sinks can be dropped");
	}
}

class gpMySQLTempSink extends gpMySQLSink {
	var $mysql;
	var $table;
	
	public function __construct( gpMySQLInserter $inserter, gpMySQL $mysql, $table ) {
		parent::__construct( $inserter );
		
		$this->mysql = $mysql;
		$this->table = $table;
	}
	
	public function drop( ) {
		$sql = "DROP TEMPORARY TABLE IF EXISTS " . $this->table; 
		
		$ok = $this->mysql->query( $sql );
		return $ok;
	}
	
	public function getTable() {
		return $this->table;
	}
	
}


class gpMySQL {
	var $connection;
	
	static function connect( $server = null, $username = null, $password = null, $new_link = false, $client_flags = 0 ) {
		$connection = mysql_connect($server, $username, $password, $new_link, $client_flags);
		$errno = mysql_errno( );
			
		if ( $errno ) {
			throw new gpClientException( "MySQL Error $errno: " . mysql_error() );
		}
		
		return new gpMySQL( $connection );
	}
	
	function __construct( $connection ) {
		if ( !$connection ) throw new Exception("connection must not be null!");
		$this->connection = $connection;
	}
	
	function gp_call_handler($gp, &$cmd, &$args, &$source, &$sink, &$capture, &$result) {
		if ( preg_match( '/-into$/', $cmd, $m ) ) {
			$cmd = preg_replace('/-into?$/', '', $cmd);
			
			$c = count($args);
			$t = $args[$c-1];
			
			$tt = preg_split('/[\s,;]+/', $t); //XXX: this is butt ugly!
			$sink = $this->make_sink( $tt[0], @$tt[1], @$tt[2] );
		} 
		
		return true;
	}
	
	function enhance_client( gpConnection $client ) {
		$h = array( $this, 'gp_call_handler' );
		$client->addCallHandler( $h );
	}
	
	function __call( $name, $args ) {
		$rc = false;
		
		//see if there's a resource in $args
		foreach ( $args as $a ) {
			if ( is_resource( $a ) ) {
				$rc = true;
				break;
			}
		}
		
		//if there was no reset in $args, add the connection
		if ( !$rc ) {
			$args[] = $this->connection;
		}
		
		$res = call_user_func_array( 'mysql_' . $name, $args );

		if ( !$res ) {
			$errno = mysql_errno( $this->connection );
			
			if ( $errno ) {
				throw new gpClientException( "MySQL Error $errno: " . mysql_error() );
			}
		}

		return $res;
	}
	
	function quote_string( $s ) {
		return "'" . mysql_real_escape_string( $s ) . "'";
	}
	
	private function next_id() {
		static $id = 1;
		
		$id++;
		return $id;
	} 
	
	public function make_temp_table( $field1, $field2 = null ) {
		$table = "gp_temp_" . $this->next_id();
		$sql = "CREATE TEMPORARY TABLE " . $table; 
		$sql .= "(";
		$sql .= $field1 . " INT NOT NULL";
		if ($field2) $sql .= ", " . $field2 . " INT NOT NULL";
		$sql .= ")";
		
		$this->query($sql);
		
		return $table;
	}

	protected function new_inserter( $table, $field1, $field2 = null ) {
		return new gpMySQLSimpleInserter( $this, $table, $field1, $field2 );
	}
	
	public function make_temp_sink( $field1, $field2 = null ) {
		$table = $this->make_temp_table($field1, $field2);
		
		$ins = $this->new_inserter($table, $field1, $field2);
		$sink = new gpMySQLTempSink( $ins, $this, $table );
		
		return $sink;
	}

	public function make_sink( $table, $field1, $field2 = null ) {
		$ins = $this->new_inserter($table, $field1, $field2);
		$sink = new gpMySQLSink( $inserter );
		
		return $sink;
	}

	public function make_source( $table, $field1, $field2 = null, $big = false ) {
		if ( $field2 ) $f = "$field1, $field2";
		else $f = "$field1";
		
		$sql = "SELECT $f FROM $table ";
		
		if ( $field2 ) $sql .= " ORDER BY $field1, $field2";
		else $sql .= " ORDER BY $field1";
		
		return $this->make_query_source( $sql, $field1, $field2, $big);
	}
	
	public function make_query_source( $query, $field1, $field2 = null, $big = false ) {
		if ($big) $res = $this->unbuffered_query($query);
		else $res = $this->query($query);
		
		$src = new gpMySQLSource( $this, $res, $field1, $field2 );
		
		return $src;
	}

	public function query_to_file( $query, $file, $remote = false ) {
		$r = $remote ? "" : "LOCAL"; //TESTME
		
		$query .= " INTO $r DATA OUTFILE "; //TESTME
		$query .= $this->quote_string($file);
		
		return $this->query($query);
	}

	public function insert_from_file( $table, $file, $remote = false ) {
		$r = $remote ? "" : "LOCAL"; //TESTME

		$query = "";
		$query .= " LOAD $r DATA INFILE "; //TESTME
		$query .= $this->quote_string($file);
		$query .= " INTO TABLE $table";
		
		return $this->query($query);
	}

}
