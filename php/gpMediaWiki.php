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
			
			#$this->dump_query( $sql );
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
		$this->table_obj->set_field_definition( $this->id_field, "INT NOT NULL");
		$this->table_obj->set_field_definition( $this->namespace_field, "INT DEFAULT NULL");
		$this->table_obj->set_field_definition( $this->title_field, "VARCHAR(255) BINARY DEFAULT NULL");
		$this->table_obj->add_key_definition( "PRIMARY KEY (" . $this->id_field . ")" );
		$this->table_obj->add_key_definition( "UNIQUE KEY (" . $this->namespace_field . ", " . $this->title_field . ")" );
		
		$this->table_id_obj = new gpMySQLTable( $this->table, $this->id_field );
		$this->table_id_obj->add_key_definition( "PRIMARY KEY (" . $this->id_field . ")" );
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
		$sql .= $this->table_obj->get_field_definitions();
		$sql .= ")";
		
		$this->glue->mysql_query($sql);
		
		$this->table = $table;
		$this->table_obj->set_name( $this->table );
		$this->table_id_obj->set_name( $this->table );

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
	
	public function delete_where( $where ) {
		$sql= "DELETE FROM " . $this->table ." ";
		$sql .= $where;
		
		return $this->glue->mysql_query( $sql );
	} 
	
	public function delete_using( $using, $tableAlias = "T" ) {
		$sql= "DELETE FROM $tableAlias ";
		$sql .= "USING " . $this->table ." AS $tableAlias ";
		$sql .= $using;
		
		return $this->glue->mysql_query( $sql );
	} 
	
	public function resolve_ids( ) {
		//NOTE: MySQL can't perform self-joins on temp tables. so we need to copy the ids to another temp table first.
		$t = new gpMySQLTable("?", "page_id");
		$t->add_key_definition("PRIMARY KEY (page_id)");
		
		$tmp = $this->glue->make_temp_table( $t );
		
		$sql = $tmp->get_insert(true);
		$sql .= "SELECT " . $this->id_field;
		$sql .= " FROM " .  $this->table;
		$sql .= " WHERE page_title IS NULL";
		
		$this->glue->mysql_query( $sql );  //copy page ids with no page title into temp table
		
		$sql = "SELECT P.page_id, P.page_namespace, P.page_title ";
		$sql .= " FROM " . $this->glue->wiki_table("page") . " AS P ";
		$sql .= " JOIN " . $tmp->get_name() . " AS T ON T.page_id = P.page_id";
		
		$this->add_from_select( $sql );
		
		$this->glue->drop_temp_table( $tmp );  
		return true;
	}

	public function make_sink() {
		$sink = $this->glue->make_sink( $this->table_obj );
		return $sink;
	}

	public function make_id_sink() {
		$sink = $this->glue->make_sink( $this->table_id_obj );
		return $sink;
	}

	public function make_id_source( $ns = null ) {
		return $this->make_source( $ns, true );
	}

	public function make_source( $ns = null, $ids_only = false ) {
		$t = $ids_only ? $this->table_id_obj : $this->table_obj;
		
		if ( $ns !== null ) {
			$select = $t->get_select();
			
			if ( is_array($ns) ) $select .= " where page_namespace in " . $this->glue->as_list( $ns ); 
			else $select .= " where page_namespace = " . (int)$ns; 
			
			$t = new gpMySQLSelect($select);
		}
		
		$src = $this->glue->make_source( $t );
		return $src;
	}

	public function capture( $ns = null, &$data = null ) {
		$sink = new gpArraySink( $data );
		$this->copy_to_sink( $ns, $sink );
		return $sink->getData();
	}

	public function capture_ids( $ns = null, &$data = null ) {
		$sink = new gpArraySink( $data );
		$this->copy_ids_to_sink( $ns, $sink );
		return $sink->getData();
	}

	public function copy_to_sink( $ns, $sink ) {
		$src = $this->make_source($ns);
		return $this->glue->copy($src, $sink, "~");
	}

	public function copy_ids_to_sink( $ns, $sink ) {
		$src = $this->make_id_source($ns);
		return $this->glue->copy($src, $sink, "~");
	}

	public function add_source( $src ) {
		$sink = $this->make_sink();
		return $this->glue->copy( $src, $sink, "+" );
	}

	public function add_page_set( $set ) {
		$select = $set->get_table()->get_select();
		return $this->add_from_select( $select );
	}

	public function subtract_page_set( $set ) {
		$t = $set->get_table();
		return $this->subtract_table( $t );
	}

	public function subtract_source( $src ) { //XXX: must be a 1 column id source...
		$t = new gpMySQLTable("?", "page_id");
		$sink = $this->glue->make_temp_sink( $t );
		$t = $sink->getTable();
		
		$this->glue->copy( $src, $sink, "+" );
		
		$ok = $this->subtract_table($t, "page_id");
		
		$this->glue->drop_temp_table($t);
		return $ok;
	}

	public function retain_page_set( $set ) {
		$t = $set->get_table();
		return $this->retain_table( $t );
	}

	public function retain_source( $src ) { //XXX: must be a 1 column id source...
		$t = new gpMySQLTable("?", "page_id");
		$sink = $this->glue->make_temp_sink( $t );
		$t = $sink->getTable();
		
		$this->glue->copy( $src, $sink, "+" );
		
		$ok = $this->retain_table($t, "page_id");
		
		$this->glue->drop_temp_table($t);
		return $ok;
	}

	public function subtract_table( $table, $id_field = null ) {
		if ( !$id_field ) $id_field = $table->get_field1();
		
		$sql = "DELETE FROM T ";
		$sql .= " USING " . $this->table . " AS T ";
		$sql .= " JOIN " . $table->get_name() . " AS R ";
		$sql .= " ON T." . $this->id_field . " = R." . $id_field;
		
		$this->glue->mysql_query($sql);
		return true;
	}

	public function retain_table( $table, $id_field = null ) {
		if ( !$id_field ) $id_field = $table->get_field1();
		
		$sql = "DELETE FROM T ";
		$sql .= " USING " . $this->table . " AS T ";
		$sql .= " LEFT JOIN " . $table->get_name() . " AS R ";
		$sql .= " ON T." . $this->id_field . " = R." . $id_field;
		$sql .= " WHERE R.$id_field IS NULL";
		
		$this->glue->mysql_query($sql);
		return true;
	}

	public function remove_page( $ns, $title ) {
		$sql = "DELETE FROM " . $this->table;
		$sql .= " WHERE " . $this->namespace_field . " = " . (int)$ns;
		$sql .= " AND " . $this->title_field . " = " . $this->glue->quote_string($title);
		
		$this->glue->mysql_query($sql);
		return true;
	}
	
	public function remove_page_id( $id ) {
		$sql = "DELETE FROM " . $this->table;
		$sql .= " WHERE " . $this->id_field . " = " . (int)$id;
		
		$this->glue->mysql_query($sql);
		return true;
	}

	public function strip_namespace( $ns, $inverse = false ) {
		$sql = "DELETE FROM " . $this->table;
		$sql .= " WHERE " . $this->namespace_field;
		
		if ( is_array($ns) ) $sql .=  ( $inverse ? " not in " : " in " ) . $this->glue->as_list( $ns ); 
		else $sql .= ( $inverse ? " != " : " = " ) . (int)$ns; 
			
		$this->glue->mysql_query($sql);
		return true;
	}

	public function retain_namespace( $ns ) {
		return $this->strip_namespace( $ns, true );
	}
	
	public function add_page( $id, $ns, $title ) {
		if ( !$id ) $id = $this->glue->get_page_id( NS_CATEGORY, $cat );
		
		$values = array($id, $ns, $title);
		
		$sql = $this->table_obj->insert_command();
		$sql .= " VALUES ";
		$sql .= $this->glue->as_list($values);
		
		$this->glue->mysql_query( $sql );
		return true;
	}

	public function add_page_id( $id ) {
		$values = array($id);
		
		$sql = "INSERT IGNORE INTO " . $this->table;
		$sql .= " ( " . $this->id_field . " ) ";
		$sql .= " VALUES ";
		$sql .= $this->glue->as_list($values);
		
		$this->glue->mysql_query( $sql );
		return true;
	}
	
	public function expand_categories( $ns = null ) {
		//NOTE: MySQL can't perform self-joins on temp tables. so we need to copy the category names to another temp table first.
		$t = new gpMySQLTable("?", "cat_title");
		$t->set_field_definition("cat_title", "VARCHAR(255) BINARY NOT NULL");
		$t->add_key_definition("PRIMARY KEY (cat_title)");
		
		$tmp = $this->glue->make_temp_table( $t );
		
		$sql = $tmp->get_insert(true);
		$sql .= " select page_title ";
		$sql .= " from " . $this->table . " as T ";
		$sql .= " where page_namespace =  ".NS_CATEGORY;
	
		$this->glue->mysql_query( $sql );
		#$this->glue->dump_query("select * from ".$tmp->get_name());
		
		// ----------------------------------------------------------
		$sql = "select P.page_id, P.page_namespace, P.page_title ";
		$sql .= " from " . $this->glue->wiki_table( "page" ) . " as P ";
		$sql .= " join " . $this->glue->wiki_table( "categorylinks" ) . " as X ";
		$sql .= " on X.cl_from = P.page_id ";
		$sql .= " join " . $tmp->get_name() . " as T ";
		$sql .= " on T.cat_title = X.cl_to ";
		
		if ($ns !== null) {
			if ( is_array($ns) ) $sql .= " where P.page_namespace in " . $this->glue->as_list( $ns ); 
			else $sql .= " where P.page_namespace = " . (int)$ns; 
		}
	
		#$this->glue->dump_query($sql);
		$this->add_from_select( $sql );
		
		#$this->glue->dump_query("select * from ".$this->table);
		$this->glue->drop_temp_table( $tmp );
		return true;
	}
	
	public function add_subcategories( $cat, $depth ) {
		$this->add_subcategory_ids($cat, $depth);
		$this->resolve_ids();
		return true;
	}
	
	protected function add_subcategory_ids( $cat, $depth ) {
		$id = $this->glue->get_page_id( NS_CATEGORY, $cat );
		if ( !$id ) return false;

		$sink = $this->make_id_sink();
		$status = $this->glue->traverse_successors( $id, $depth, $sink );
		$sink->close();
		return true;
	}
	
	public function add_pages_in( $cat, $ns, $depth ) {
		if ( !$this->add_subcategories($cat, $depth) ) return false;

		$this->expand_categories($ns);
		return true;
	}

	public function add_pages_transclusing( $tag, $ns = null ) {
		if ( $ns === null ) $ns = NS_TEMPLATE;
		$tag = $this->glue->get_db_key( $tag );

		$sql = " SELECT page_id, page_namespace, page_title ";
		$sql .= " FROM " . $this->glue->wiki_table( "page" );
		$sql .= " JOIN " . $this->glue->wiki_table( "templatelinks" );
		$sql .= " ON tl_from = page_id ";
		$sql .= " WHERE tl_namespace = " . (int)$ns;
		$sql .= " AND tl_title = " . $this->glue->quote_string($tag);
		
		return $this->add_from_select($sql);
	}

	public function clear() {
		$sql = "TRUNCATE " . $this->table;
		$this->glue->mysql_query($sql);
		return true;
	}

	public function dispose() {
		$sql = "DROP TEMPORARY TABLE " . $this->table;
		$this->glue->mysql_query($sql);
		return true;
	}
}

