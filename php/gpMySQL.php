<?php

class gpMySQLSource extends gpDataSource {
	var $connection;
	var $result;
	var $field1;
	var $field2;
	
	public function __construct( $connection, $result, $field1, $field2 = null) {
		$this->connetion = $connection;
		$this->result = $result;
		$this->field1 = $field1;
		$this->field2 = $field2;
	}

	public function nextRow() {
		$raw = mysql_fetch_assoc( $this->result );
		$errno = $res ? false : mysql_errno( $this->connection );
		
		if ( $errno ) {
			throw new gpClientException( "MySQL Error $errno: " . mysql_error() );
		}
		
		if ( !$raw ) return null;
		 
		$row = array();
		$row[] = $raw[ $this->field1 ];
		
		if ( $this->field2 ) {
			$row[] = $raw[ $this->field2 ];
		}
				
		return $row;
	}
	
	public function close( ) {
		mysql_free_result( $this->result ); 
	}
}

abstract class gpMySQLInserter {
	var $connection;
	var $table;
	var $fields;

	function __construct ( $connection, $table, $field1 = null, $field2 = null ) {
		$this->conenction = $connection;
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
	
	protected function query( $sql ) {
		$res = mysql_query( $sql, $this->connection );
		$errno = $res ? false : mysql_errno( $this->connection );
		
		if ( $errno ) {
			throw new gpClientException( "MySQL Error $errno: " . mysql_error() );
		}
		
		return $res;
	}
}

class gpMySQLSimpleInserter {

	protected function as_list( $values ) {
		$sql = "(";

		$first = true;
		foreach ( $values as $v ) {
			if ( !$first ) $sql .= ",";
			else $first = false;
			
			if ( is_null($v) ) $sql.= "NULL";
			else if ( is_int($v) ) $sql.= $v;
			else if ( is_float($v) ) $sql.= $v;
			else if ( is_str($v) ) $sql.= '"' . mysql_real_escape_string($v) . '"'; //TODO: charset...
			else throw new gpUsageException("bad value type: " . gettype($v));
		}
		
		$sql .= ")";
		
		return $sql;
	}
	
	protected function insert_command( ) {
		$sql = "INSERT IGNORE INTO {$this->table} ";
		
		if ( $this->fields ) {
			$sql .= " ( ". implode(",", $this->fields) ." ) "
		}
		
		return $sql;
	}
	
	public function insert( $values ) {
		$sql = $this->insert_command();
		$sql .= " VALUES ";
		$sql .= $this->as_list($values);
		
		$this->query( $sql );
	}

}

class gpMySQLSink extends gpDataSink {
	
	public function __construct( $inserter ) {
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

class gpMySQLTempSink extends gpDataSink {
	var $connection;
	var $table;
	
	public function __construct( $inserter, $connection, $table ) {
		parent::__construct( $inserter );
		
		$this->connection = connection;
		$this->table = table;
	}
	
	public function drop( ) {
		$sql = "DROP TEMPORARY TABLE IF EXISTS " . $this->table; 
		
		$ok = mysql_query( sql );
		$errno = $ok ? false : mysql_errno( $this->connection );
		
		if ( $errno ) {
			throw new gpClientException( "MySQL Error $errno: " . mysql_error() );
		}
	}
	
	public function get_table() {
		return $this->table;
	}
	
}


class gpMySQL {
	var $connection;
	
	function __construct( $connection ) {
		$this->conenction = $connection;
	}
	
	function call__( $name, $args ) {
		$args[] = $this->connection;
		call_user_func_array( 'mysql_' . $name, $args );
	}
	
	private function next_id() {
		static $id = 1;
		
		$id++;
		return $id;
	} 
	
	public function make_temp_table( $field1, $fields2 = null ) {
		$table = "gp_temp_" . $this.>next_id();
		$sql = "CREATE TEMPORARY TABLE " . $table; 
		$sql .= "(";
		$sql .= $field1 . " INT NOT NULL";
		if ($field2) $sql .= ", " . $field2 . " INT NOT NULL";
		$sql .= ")";
		
		$this->query($sql);
		
		return $table;
	}

	public function query( $sql ) {
		$res = mysql_query( $sql, $this->connection );
		$errno = $res ? false : mysql_errno( $this->connection );
		
		if ( $errno ) {
			throw new gpClientException( "MySQL Error $errno: " . mysql_error() );
		}
		
		return $res;
	}

	protected function new_inserter( $table, $field1, $field2 = null ) {
		return new gpMySQLSimpleInserter( $this->connection, $table, $field1, $field2 );
	}
	
	public function make_temp_sink( $cols ) {
		$field1 = "n";
		if ($cols>1) $field2 = "m";
		
		$table = $this->make_temp_table($field1, $field2);
		
		$ins = $this->new_inserter($table, $field1, $field2);
		$sink = new gpMySQLTempSink( $inserter, $this->conenction, $table );
		
		return $sink;
	}

	public function make_sink( $table, $cols ) {
		$ins = $this->new_inserter($table, $field1, $field2);
		$sink = new gpMySQLSink( $inserter );
		
		return $sink;
	}

	public function make_source( $table, $field1, $field2 = null ) {
		if ( $field2 ) $f = "($field1, $field2)";
		else $f = "($field1)";
		
		$sql = "SELECT $f FROM $table ";
	}
	
	public function make_query_source( $query, $field1, $field2 = null ) {
		$res = $this->query($query);
		$src = new gpMySQLSource( $this->connection, $res, $field1, $field2 );
		
		return $src;
	}

	public function query_to_file( $query, $file, $remote = false ) {
		$r = $remote ? "" : "LOCAL"; //TESTME
		
		$query .= " INTO $r DATA OUTFILE "; //TESTME
		$query .= '"' . mysql_real_escape_string($file) . '"';
		
		return $this->query($query);
	}

	public function insert_from_file( $table, $file, $remote = false ) {
		$r = $remote ? "" : "LOCAL"; //TESTME

		$query = "";
		$query .= " LOAD $r DATA INFILE "; //TESTME
		$query .= '"' . mysql_real_escape_string($file) . '"';
		$query .= " INTO TABLE $table";
		
		return $this->query($query);
	}

}
