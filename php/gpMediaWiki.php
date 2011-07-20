<?php
require_once(dirname(__FILE__) . "/gpMySQL.php");

if ( !defined('NS_MAIN') ) define( 'NS_MAIN', 0 );
if ( !defined('NS_TALK') ) define( 'NS_TALK', 1 );
if ( !defined('NS_USER') ) define( 'NS_USER', 2 );
if ( !defined('NS_USER_TALK') ) define( 'NS_USER_TALK', 3 );
if ( !defined('NS_PROJECT') ) define( 'NS_PROJECT', 4 );
if ( !defined('NS_PROJECT_TALK') ) define( 'NS_PROJECT_TALK', 5 );
if ( !defined('NS_FILE') ) define( 'NS_FILE', 6 );
if ( !defined('NS_FILE_TALK') ) define( 'NS_FILE_TALK', 7 );
if ( !defined('NS_MEDIAWIKI') ) define( 'NS_MEDIAWIKI', 8 );
if ( !defined('NS_MEDIAWIKI_TALK') ) define( 'NS_MEDIAWIKI_TALK', 9 );
if ( !defined('NS_TEMPLATE') ) define( 'NS_TEMPLATE', 10 );
if ( !defined('NS_TEMPLATE_TALK') ) define( 'NS_TEMPLATE_TALK', 11 );
if ( !defined('NS_HELP') ) define( 'NS_HELP', 12 );
if ( !defined('NS_HELP_TALK') ) define( 'NS_HELP_TALK', 13 );
if ( !defined('NS_CATEGORY') ) define( 'NS_CATEGORY', 14 );
if ( !defined('NS_CATEGORY_TALK') ) define( 'NS_CATEGORY_TALK', 15 );


class gpMediaWikiGlue extends gpMySQLGlue {
	var $table_prefix = null;
	
	function __construct( $transport ) {
		parent::__construct($transport);

		$h = array( $this, 'gp_mediawiki_exec_handler' );
		$this->addExecHandler( $h );
	}
	
	public function set_table_prefix( $prefix ) {
		$this->table_prefix = $prefix;
	}
	
	public function gp_mediawiki_exec_handler( $glue, &$command, &$source, &$sink, &$has_output, &$status ) {
		if ( preg_match('/^wiki-(.*)$/', $command[0], $m) ) {
			$name = str_replace('-', '_', $m[0]);
			$args = array_slice( $command, 1 );
			$args[] = $sink;
			
			$has_output = true;
			$status = call_user_func_array( array( $this, "{$name}_impl"), $args );
			return false;
		} 

		return true;
	}
	
	public function get_db_key( $name ) { 
		//TODO: use native MediaWiki method if available
		$name = trim($name);
		$name = str_replace(' ', '_', $name);
		$name = ucfirst( $name ); //FIXME: unreliable
		return $name;
	}

	public function wiki_table( $name ) {
		return $this->table_prefix . $name;
	}
	
	public function get_page_id( $ns, $title ) {
		$sql = "select page_id from " . $this->wiki_table( "page" );
		$sql .= " where page_namespace = " . (int)$ns;
		$sql .= " and page_title = " . $this->quote_string( $this->get_db_key($title) );
		
		$id = $this->mysql_query_value( $sql );
		return $id;
	}
	
	public function add_arcs_from_category_structure( ) {
		$sql = "select C.page_id as parent, P.page_id as child";
		$sql .= " from " . $this->wiki_table( "page" ) . " as P ";
		$sql .= " join " . $this->wiki_table( "categorylinks" ) . " as X ";
		$sql .= " on X.cl_from = P.page_id ";
		$sql .= " join " . $this->wiki_table( "page" ) . " as C ";
		$sql .= " on C.page_namespace = " . NS_CATEGORY;
		$sql .= " and C.page_title = X.cl_to ";
		$sql .= " where P.page_namespace = " . NS_CATEGORY;
		
		$src = $this->make_source( new gpMySQLSelect( $sql ) );
		
		$this->add_arcs( $src );
	}
	 
	public function wiki_subcategories_impl( $cat, $depth, gpDataSink $sink ) {
		$id = $this->get_page_id( NS_CATEGORY, $cat );

		if ( !$id ) return 'NONE';

		$temp = $this->make_temp_sink( new gpMySQLTable('?', 'id') );
		
		$status = $this->traverse_successors( $id, $depth, $temp );
		$temp->close();
		
		if ( $status == 'OK' ) {
			$sql = "select page_title ";
			$sql .= " from " . $this->wiki_table( "page" );
			$sql .= " join " . $temp->getTable()->get_name();
			$sql .= " on id = page_id ";
			$sql .= " where page_namespace = " . NS_CATEGORY; // should be redundant
			$sql .= " order by page_id ";
			
			$this->select_into( $sql , $sink);
		}
		
		$temp->drop();
		
		return $status;
	}

	public function wiki_pages_in( $cat, $ns, $depth, gpDataSink $sink ) {
		$id = $this->get_page_id( NS_CATEGORY, $cat );

		if ( !$id ) return 'NONE';

		$temp = $this->make_temp_sink( new gpMySQLTable('?', 'id') );
		
		$status = $this->traverse_successors( $id, $depth, $temp );
		$temp->close();
		
		if ( $status == 'OK' ) {
			//XXX: realy use a quadrupel join? or inject category names into temp table first?
			$sql = "select P.page_namespace, P.page_title ";
			$sql .= " from " . $this->wiki_table( "page" ) . " as P ";
			$sql .= " join " . $this->wiki_table( "categorylinks" ) . " as X ";
			$sql .= " on X.cl_from = P.page_id ";
			$sql .= " join " . $this->wiki_table( "page" ) . " as C ";
			$sql .= " on C.page_namespace = " . NS_CATEGORY . " and C.page_title = X.cl_to ";
			$sql .= " join " . $temp->getTable()->get_name() . " as T ";
			$sql .= " on T.id = C.page_id ";
			
			if ($ns !== null) {
				if ( is_array($sql) ) $sql .= " where page_namespace in " . $this->as_list( $ns ); 
				else $sql .= " where page_namespace = " . (int)$ns; 
			}
			
			$sql .= " order by P.page_id ";
			
			$this->dump_query( $sql );
			$this->select_into( $sql , $sink);
		}
		
		$temp->drop();
		
		return $status;
	}

	/*
	public function update_successors( int $page_id ) {
		$sql = "";
		$src = $this->make_source( new gpMySQLSelect( $sql ) );
		$this->replace_successors( $page_id );
	}
	*/
	 
	public static function new_client_connection( $graphname, $host = false, $port = false ) {
		return new gpMediaWikiGlue( new gpClientTransport($graphname, $host, $port) );
	}

	public static function new_slave_connection( $command, $cwd = null, $env = null ) {
		return new gpMediaWikiGlue( new gpSlaveTransport($command, $cwd, $env) );
	}
}


class gpPageSet {
	var $glue;
	var $table;
	var $id_field;
	var $title_field;
	var $namespace_field;
	
	public function __construct( $glue, $table = "?", $id_field = "page_id", $namespace_field = "page_namespace", $title_field = "page_title" ) {
		$this->glue = $glue;
		$this->table = $table;
		
		$this->id_field = $id_field;
		$this->namespace_field = $namespace_field;
		$this->title_field = $title_field;
		
		$this->table_obj = new gpMySQLTable( $this->table, $this->id_field, $this->namespace_field, $this->title_field );
		$this->table_id_obj = new gpMySQLTable( $this->table, $this->id_field );
	}
	
	public function get_table() {
		return $this->table_obj;
	}
	
	public function create_table( ) {
		$table = $this->table;
		$t = "";
		
		if ( !$table || $table === '?' ) { 
			$table = "gp_temp_" . $this->glue->next_id();
			$t = " TEMPORARY ";
		}
		
		$sql = "CREATE $t TABLE " . $table; 
		$sql .= "(";
		$sql .= $this->id_field . " INT NOT NULL,";
		$sql .= $this->namespace_field . " INT DEFAULT NULL,";
		$sql .= $this->title_field . " VARCHAR(255) BINARY  DEFAULT NULL,";
		$sql .= " PRIMARY KEY (" . $this->id_field . "),";
		$sql .= " UNIQUE KEY (" . $this->namespace_field . ", " . $this->title_field . ")";
		$sql .= ")";
		
		print "*** $sql ***";
		$this->glue->mysql_query($sql);
		
		$this->table = $table;
		$this->table_obj = new gpMySQLTable( $this->table, $this->id_field, $this->namespace_field, $this->title_field );
		$this->table_id_obj = new gpMySQLTable( $this->table, $this->id_field );

		return $table;  
		
	}
	
	public function add_from_select( $select ) {
		$sql= "REPLACE INTO " . $this->table ." ";
		$sql .= "( ";
		$sql .= $this->id_field . ", ";
		$sql .= $this->namespace_field . ", ";
		$sql .= $this->title_field . " ) ";
		$sql .= $select;
		
		return $this->glue->mysql_query( $sql );
	} 
	
	public function resolve_ids( ) {
		$sql = "SELECT P.page_id, P.page_namespace, P.page_title ";
		$sql .= " FROM " . $this->glue->wiki_table("page") . " AS P ";
		$sql .= " JOIN " . $this->table . " AS T ON T." . $this->id_field . " = P.page_id";
		$sql .= " WHERE T.page_title IS NULL";
		
		$this->add_from_select( $sql );
	}

	public function make_sink() {
		$sink = $this->glue->make_sink( $this->table_obj );
		return $sink;
	}

	public function make_id_sink() {
		$sink = $this->glue->make_sink( $this->table_id_obj );
		return $sink;
	}

	public function make_source() {
		$src = $this->glue->make_source( $this->table_obj );
		return $src;
	}

	public function capture( &$data = null ) {
		$sink = gpArraySink( $data );
		$this->capture_into( $sink );
		return $sink->getData();
	}

	public function capture_ids( &$data = null ) {
		$sink = gpArraySink( $data );
		$this->capture_into( $sink );
		return $sink->getData();
	}

	public function capture_into( $sink ) {
		$src = $this->make_source();
		$this->glue->copy($src, $sink, "~");
	}

	public function capture_ids_into( $sink ) {
		$src = $this->make_id_source();
		$this->glue->copy($src, $sink, "~");
	}

	public function add_from_source( $src ) {
		$sink = $this->make_sink();
		return $this->glue->copy( $src, $sink, "+" );
	}

	public function add_from_page_set( $set ) {
		$src = $set->make_source();
		$sink = $this->make_sink();
		return $this->glue->copy( $src, $sink, "+" );
	}

	public function add_page( $id, $ns, $title ) {
		if ( !$id ) $id = $this->glue->get_page_id( NS_CATEGORY, $cat );
		
		$values = array($id, $ns, $title);
		
		$sql = $this->table_obj->insert_command();
		$sql .= " VALUES ";
		$sql .= $this->glue->as_list($values);
		
		$this->glue->mysql_query( $sql );
	}

	public function add_page_id( $id ) {
		$values = array($id);
		
		$sql = "INSERT IGNORE INTO " . $this->table;
		$sql .= " ( " . $this->id_field . " ) ";
		$sql .= " VALUES ";
		$sql .= $this->glue->as_list($values);
		
		$this->glue->mysql_query( $sql );
	}
	
	public function expand_categories( $ns ) {
		$sql = "select P.page_namespace, P.page_title ";
		$sql .= " from " . $this->wiki_table( "page" ) . " as P ";
		$sql .= " join " . $this->wiki_table( "categorylinks" ) . " as X ";
		$sql .= " on X.cl_from = P.page_id ";
		$sql .= " join " . $this->table . " as T ";
		$sql .= " on T.page_namespace =  ".NS_CATEGORY." and T.page_title = X.cl_to ";
		
		if ($ns !== null) {
			if ( is_array($sql) ) $sql .= " where page_namespace in " . $this->as_list( $ns ); 
			else $sql .= " where page_namespace = " . (int)$ns; 
		}
	
		$this->add_from_select( $sql );
	}
	
	public function add_pages_in( $cat, $ns, $depth ) {
		$id = $this->glue->get_page_id( NS_CATEGORY, $cat );
		if ( !$id ) return false;

		$sink = $this->make_id_sink();
		$status = $this->glue->traverse_successors( $id, $depth, $sink );
		$sink->close();

		$this->resolve_ids();
		$this->expand_categories($ns);
	}

}

