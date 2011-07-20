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

	public function get_table_name( $name ) {
		return $this->table_prefix . $name;
	}
	
	public function get_page_id( $ns, $title ) {
		$sql = "select page_id from " . $this->get_table_name( "page" );
		$sql .= " where page_namespace = " . (int)$ns;
		$sql .= " and page_title = " . $this->quote_string( $this->get_db_key($title) );
		
		$id = $this->mysql_query_value( $sql );
		return $id;
	}
	
	public function add_arcs_from_category_structure( ) {
		$sql = "select C.page_id as parent, P.page_id as child";
		$sql .= " from " . $this->get_table_name( "page" ) . " as P ";
		$sql .= " join " . $this->get_table_name( "categorylinks" ) . " as X ";
		$sql .= " on X.cl_from = P.page_id ";
		$sql .= " join " . $this->get_table_name( "page" ) . " as C ";
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
			$sql .= " from " . $this->get_table_name( "page" );
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
			$sql .= " from " . $this->get_table_name( "page" ) . " as P ";
			$sql .= " join " . $this->get_table_name( "categorylinks" ) . " as X ";
			$sql .= " on X.cl_from = P.page_id ";
			$sql .= " join " . $this->get_table_name( "page" ) . " as C ";
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
