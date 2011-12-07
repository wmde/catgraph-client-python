<?php

require_once("gpMediaWiki.php");

#$config = parse_ini_file("CatGraphApi.config.php");
$config = array(
	'bluedev' => array(
		'gp-host' => 'localhost',
		'gp-graph' => 'bluespice',

		'mysql-host' => 'localhost',
		'mysql-user' => 'daniel',
		'mysql-password' => 'foo',

		'mysql-database' => 'bluespice',
		'mysql-prefix' => '',
	),
);

class JsonOutput {
	
	function escape($value) { # list from www.json.org: (\b backspace, \f formfeed)
		$escapers = array("\\", "/", "\"", "\n", "\r", "\t", "\x08", "\x0c"); 
		$replacements = array("\\\\", "\\/", "\\\"", "\\n", "\\r", "\\t", "\\f", "\\b"); 
		$result = str_replace($escapers, $replacements, $value); 
		return $result; 
	}

	function output_start() {
		print "[\n";
	}

	function output_title_item( $title ) {
		print "\t'" . $this->escape($title) . "',\n";
		
		#print "\t{ id: " . $this->escape($id) . ", name: " . $this->escape($name) . " }\n";
	}

	function output_end() {
		print "]\n";
	}
}

function die_with_error( $msg ) {
	die($msg);
}

function create_gp_client( $wiki ) {
	global $config;
	
	if ( !isset( $config[ $wiki ] ) ) {
		die_with_error("unknown wiki: $wiki");
	}
	
	$gp_graph = $config[ $wiki ][ 'gp-graph' ];
	$gp_host = @$config[ $wiki ][ 'gp-host' ];
	$gp_port = @$config[ $wiki ][ 'gp-port' ];

	$gp = gpMediaWikiGlue::new_client_connection( $gp_graph, $gp_host, $gp_port ); #todo: optional auth
	$gp->connect();
	
	$v = $gp->protocol_version();
	
	$mysql_host = @$config[ $wiki ][ 'mysql-host' ];
	$mysql_user = $config[ $wiki ][ 'mysql-user' ];
	$mysql_password = @$config[ $wiki ][ 'mysql-password' ];

	$mysql_database = $config[ $wiki ][ 'mysql-database' ];
	$mysql_prefix = @$config[ $wiki ][ 'mysql-prefix' ];

	$gp->mysql_connect($mysql_host, $mysql_user, $mysql_password);
	$gp->mysql_select_db($mysql_database);
	$gp->prefix = $mysql_prefix;
	
	return $gp;
}

function create_output_handler( $format ) {
	return new JsonOutput();
}

function list_subcategories( $gp, $cat, $depth, $output ) {
	if ( !$depth ) $depth = 10;
	
	$cats = $gp->get_subcategories($cat, $depth);

	$output->output_start();
	
	foreach ( $cats as $row ) {
		$output->output_title_item( $row[0] );
	} 
	
	$output->output_end();
}

function list_path_to_root( $gp, $cat, $output ) {
}

if ( @$_REQUEST['op'] ) {
	$wiki = $_REQUEST['wiki'];
	$op = $_REQUEST['op'];
	$cat = @$_REQUEST['cat'];
	$format = @$_REQUEST['format'];
	$depth = @$_REQUEST['depth'];
	
	$gp = create_gp_client( $wiki );
	$output = create_output_handler( $format );
	
	if ( $op == 'subcats' ) {
		list_subcategories( $gp, $cat, $depth, $output );
	} else if ( $op == 'ptr' ) {
		list_path_to_root( $gp, $cat, $output );
	}
	
	exit();
}

?>
<html>
<head>
	<title>CatGraphApi</title>
</head>
<body>
	<h1>CatGraphApi</h1>
	
	<form>
		<p>
			<label for="wiki">Wiki</label>
			<select name="wiki">
				<option value="bluedev">bluedev</option>
			</select>
		</p>
		<p>
			<label for="format">Format</label>
			<select name="format">
				<option value="json">json</option>
			</select>
		</p>
		<p>
			<label for="op">Operation</label>
			<select name="op">
				<option value="subcats">Subcategories</option>
			</select>
		</p>
		<p>
			<label for="cat">Category</label>
			<input type="text" name="cat" value=""/>
		</p>
		<p>
			<input type="submit" value="go"/>
		</p>
	</form>
</body>
</html>
