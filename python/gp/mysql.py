from client import *

import re
import MySQLdb

class MySQLSource (DataSource):
    
    def __init__(self, glue, result, table):
        self.glue = glue
        self.result = result
        self.table = table
    

    def nextRow (self):
        raw = self.glue.mysql_fetch_assoc( self.result )
        
        if not raw: return None
        
        row = []
        
        for f in self.table.get_fields():
            row.append( raw.get( f ) )
        
                
        return tuple( row )
    
    
    def close (self):
        self.glue.mysql_free_result( self.result )
    

def strip_qualifier(self, n ):
    return re.sub(r'^.*\.', '', n)

class MySQLTable:

    def __init__(self, name):
        self.name = name
        
        self.field_definitions = []
        self.key_definitions = []
        
        args = func_get_args()
        
        if ( isinstance(args[1], (tuple, list) ) ): self.fields = args[1]
        else: self.fields = args[1:]
        
        for f in self.fields:
            if ( not f ): raise gpUsageException( "empty field name!" )
        
        
        #for ( i = count(self.fields) -1; i >= 0; i-- ):
            #if ( self.fields[i] ) break
        
        
        #if  i+1 < count(self.fields) :
            #self.fields = array_slice(self.fields, 0, i+1)
    
    
    def set_name( self, name ):
        self.name = name
    
    
    def set_fields(self, field ):
        self.fields = fields
    
    
    def set_field_definition(self, field, decl ):
        self.field_definitions[field] = decl
    
    
    def add_key_definition(self, keyDef ):
        self.key_definitions.append( keyDef )
    
    
    def get_name(self,):
        return self.name
      

    
    def get_field(self, n ):
        return self.fields.get( n-1 )
    
    
    def get_field1(self, basename_only = False ):
        if ( basename_only ): return strip_qualifier( self.get_field(1) )
        else: return self.get_field(1)
    
    
    def get_field2(self, basename_only = False ):
        if ( basename_only ): return strip_qualifier( self.get_field(2) )
        else: return self.get_field(2)
    

    def get_fields(self,):
        return self.fields
      

    def get_field_list(self,):
        return implode(", ", self.fields)
      
    
    def get_field_definitions(self,):
        s = ""
        
        for f in self.fields:
            if ( not f ): continue #XXX: should not happen!
            if ( not s) : s+= ", "
            
            if ( not self.field_definitions[f]) : s += f + " %s" % self.field_definitions[f]
            else: s += f + " INT NOT NULL "
        
        
        for k in self.key_definitions:
            if ( not s): s+= ", "
            s += k
        

        return s
    

    def _get_select(self,):
        return "SELECT " + self.get_field_list() + " FROM " + self.get_name()
      

    def get_insert(self, ignore = False ):
        ig = "IGNORE" if ignore else ""
        return "INSERT ig INTO " + self.get_name() + " ( " + self.get_field_list() + " ) "
      

    def get_order_by(self,):
        return "ORDER BY %s" % self.get_field_list()
      
    


class MySQLSelect (MySQLTable):
   
    def __init__(self, select):
        m = re.search(r'^\s*select\s+(.*?)\s+from\s+([^ ]+)(?:\s+(.*))?', select, re.INSENSITIVE + re.SINGLE_LINE)
        
        if m:
            self.select = select
            
            n = m.group(2)
            ff = re.split(r'\s*,\s*', m.group(1) )
            
            for i, f in ff:
                f = re.sub(r'^.*\s+AS\s+', '', f, re.INSENSITIVE) # use alias if defined
                ff[i] = f
            
            
            super(MySQLSelect,self).__init__(n, ff)
        else:
            raise gpUsageException("can't parse statement: %s" % select)
        
    

    def _get_select(self,):
        return self.select
      

    def get_insert(self, ignore = False ):
        raise gpUsageEsxception("can't create insert statement for: %s" % self.select)
      


class MySQLInserter:
    def __init__ ( self, glue, table ):
        self.glue = glue
        self.table = table
        self.fields = None
    
    def insert(self, values ):
        raise Error("abstract method")

    def flush (self):
        pass
    
    def close (self):
        self.flush()
    


class MySQLSimpleInserter (MySQLInserter):

    def as_list (self, values ):
        return self.glue.as_list( values )
    
    
    def _insert_command(self):
        return self.table.get_insert()
    
    
    def insert (self, values ):
        sql = self._insert_command()
        sql += " VALUES "
        sql += self.as_list(values)
        
        self.glue.mysql_query( sql )
    



class MySQLBufferedInserter (MySQLSimpleInserter):

    def __init__(self, glue, table ):
        super(MySQLBufferedInserter,self).__init__(glue, table)
        self.buffer = ""
    

    def insert (self, values ):
        vlist = self.as_list(values)
        max = self.glue.get_max_allowed_packet()

        if not self.buffer and ( strlen(self.buffer) + strlen(vlist) + 2 ) >= max  :
            self.flush()
        
        
        if self.buffer :
            self.buffer = self._insert_command()
            self.buffer += " VALUES "
        else:
            self.buffer += ", "
        
        
        self.buffer += vlist

        if strlen(self.buffer) >= max :
            self.flush()
        
    
    
    def flush (self):
        if not  self.buffer  :
            #print "*** {self.buffer ***"
            self.glue.mysql_query( self.buffer )
            self.buffer = ""
        
    



class MySQLSink (DataSink):
    
    def __init__(self, inserter ):
        self.inserter = inserter
    
    
    def putRow (self, row ):
        self.inserter.insert( row )
    
    
    def flush (self):
        self.inserter.flush()
    
    
    def close (self):
        super(MySQLSink, self).close()
        self.inserter.close()
    
    
    def drop (self):
        raise gpUsageException("only temporary sinks can be dropped")
    


class MySQLTempSink (MySQLSink):
    def __init__( self, inserter, glue, table ):
        super(MySQLTempSink, self).__init__(inserter)
        
        self.glue = glue
        self.table = table
    
    
    def drop (self):
        sql = "DROP TEMPORARY TABLE IF EXISTS %s" % self.table.get_name()
        
        ok = self.glue.mysql_query( sql )
        return ok
    
    
    def getTable (self):
        return self.table
    

    def getTableName (self):
        return self.table

class MySQLGlue (Connection):
    
    def __init__(self, transport ):
        super(MySQLGlue, self).__init__(transport)
        
        self.connection = None
        self.unbuffered = False

        self.addCallHandler( self.gp_mysql_call_handler )
    
    
    def set_unbuffered(self, unbuffered ):
        self.unbuffered = unbuffered
    
    
    def mysql_connect(self, server = None, username = None, password = None, db = None ):
        #FIXME: connection charset, etc!
        
        try:
            self.connection = MySQLdb.connect(host=server, user=username, passwd=password, db=db)
        except MySQLdb.Error, e:
            try:
                raise gpClientException( "Failed to connect! MySQL Error %s: %s" % (e.args[0], e.args[1]) )
            except IndexError:
                raise gpClientException( "Failed to connect! MySQL Error: %s" % e )
        
        if not self.connection :
            raise gpClientException( "Failed to connect! (unknown error)" )
        
        return True
    

    def set_mysql_connection(self, connection ):
        self.connection = connection
    
    
    def gp_mysql_call_handler( self, gp, params ):
        # params: cmd, args, source, sink, capture, result

        cmd = params['command']
        args = params['arguments']
        source = params['source']
        sink = params['sink']
        capture = params['capture']
        result = params['result']
            
        m = re.search( r'-(from|into)', cmd )
            
        if m:
            cmd = re.sub(r'-(from|into)?', '', cmd)
            action = m.group(1)
            
            c = count(args)
            if not c :
                raise gpUsageException("expected last argument to be a table spec; %s" % args)
            
            
            t = args[c-1]
            args = args[0:c-1]
            
            if isinstance(t, (str, unicode)) :
                if ( re.search( r'^.*select\s+i', t, re.INSENSITIVE) ): t = MySQLSelect(t)
                else: t = re.split( r'\s+|\s*,\s*', t )
            
            
            if ( isinstance(t, (list, tuple)) ): t = MySQLTable( t[0], t[1:] )
            if ( not isinstance(t, MySQLTable) ): raise gpUsageException("expected last argument to be a table spec; found %s" % get_class(t))
            
            if action == 'into' :
                if ( not t.get_name()  or  t.get_name() == "?" ): sink = self.make_temp_sink( t )
                else: sink = self.make_sink( t )
                
                result = sink #XXX: quite useless, but consistent with -from
            else:
                source = self.make_source( t )
                
                result = source #XXX: a bit confusing, and only useful for temp sinks
            
        params['command'] = cmd
        params['arguments'] = args
        params['source'] = source
        params['sink'] = sink
        params['capture'] = capture
        params['result'] = result
            
        return True
    
    
    def __make_mysql_closure( self, name, args ):
        rc = False
        
        if method_exists( this, name ) :
            return call_user_func_array( array(this, cmd), args )
        
        
        if self.unbuffered  and  name == 'mysql_query' :
            name = 'mysql_unbuffered_query'
        
        
        res = call_user_func_array( name, args )

        if not res :
            errno = mysql_errno( self.connection )
            
            if errno :
                msg = "MySQL Error %i: %s" % (errno, mysql_error())
                
                if name == 'mysql_query'  or  name == 'mysql_unbuffered_query' :
                    sql = str_replace('/\s+/', ' ', args[0])
                    if ( strlen(sql) > 255 ): sql = substr(sql, 0, 252) + '+..'
                    
                    msg+= "\nQuery was: %s" % sql
                
                
                raise gpClientException( msg )
            
        

        return res
    
    
    def __call(self, name, args ): #FIXME!
        if name.startswith('mysql_'):
            return self.__call_mysql(name, args)
        else:
            return super(MySQLGlue, self).__call(name, args)
    
    
    def quote_string (self, s ): #TODO: charset
        return "'" + mysql_real_escape_string( s ) + "'"
    
    
    def as_list (self, values ):
        sql = "("

        first = True
        for v in values:
            if ( not first ): sql += ","
            else: first = False
            
            t = type(v)
            if ( t is NoneType ): sql+= "None"
            elif ( t == int ): sql+= v
            elif ( t == float ): sql+= v
            elif ( t == str or t == unicode ): sql+= self.quote_string(v) #TODO: charset...
            else: raise gpUsageException("bad value type: %s" % gettype(v))
        
        
        sql += ")"
        
        return sql
    
    id = 1
    
    def next_id (self):
        id += 1
        return id
     
    
    def drop_temp_table (self, spec ):
        sql = "DROP TEMPORARY TABLE %s" % spec.get_name()
        self.mysql_query(sql)
    
    
    def make_temp_table (self, spec ):
        table = spec.get_name()
        
        if ( not table  or  table == '?' ):
            table = "gp_temp_%s" % self.next_id()
        
        sql = "CREATE TEMPORARY TABLE %s" % table
        sql += "("
        sql += spec.get_field_definitions()
        sql += ")"
        
        self.mysql_query(sql)
        
        return MySQLTable(table, spec.get_fields())
    

    def mysql_query_value (self, sql ):
        res = self.mysql_query( sql )
        a = self.mysql_fetch_row( res )
        self.mysql_free_result( res )
        
        if ( not a ): return None
        else: return a[0]
    
    
    def set_max_allowed_packet (self, size ):
        self.max_allowed_packet = size
    
    
    def get_max_allowed_packet (self):
        if  self.max_allowed_packet  :
            self.max_allowed_packet = self.mysql_query_value("select @@max_allowed_packet")
        

        if  self.max_allowed_packet  :
            self.max_allowed_packet = 16 * 1024 * 1024 #fall back to MySQL's default of 16MB
        
        
        return self.max_allowed_packet
    

    def select_into (self, query, sink ):
        if isinstance(query, (str, unicode)) :
            table = MySQLSelect( query )
            sql = query
        else:
            table = query
            sql = src._get_select()
        
        
        res = self.mysql_query( sql )
        src = MySQLSource( this, res, table )
        
        c = self.copy( src, sink, '+' )
        src.close()
        
        return c
    
    
    def _new_inserter(self, table ):
        return MySQLBufferedInserter( this, table )
    
    
    def make_temp_sink (self, table ):
        table = self.make_temp_table(table)
        
        ins = self._new_inserter(table)
        sink = MySQLTempSink( ins, this, table )
        
        return sink
    

    def make_sink (self, table ):
        inserter = self._new_inserter(table)
        sink = MySQLSink( inserter )
        
        return sink
    

    def make_source (self, table, big = False ):
        sql = table._get_select()
        
        if not re.search(r'\s+ORDER\s+BY\s+', sql, re.INSENSITIVE) :
            sql += ' ' + table.get_order_by()
        
        
        if (big): res = self.mysql_unbuffered_query(sql)
        else: res = self.mysql_query(sql)
        
        src = MySQLSource( this, res, table )
        return src
    

    def query_to_file (self, query, file, remote = False ):
        r = "" if remote else "LOCAL" #TESTME
        
        query += " INTO %s DATA OUTFILE " % r #TESTME
        query += self.quote_string(file)
        
        return self.mysql_query(query)
    

    def insert_from_file (self, table, file, remote = False ):
        r = "" if remote else "LOCAL" #TESTME

        query = ""
        query += " LOAD %s DATA INFILE " % r #TESTME
        query += self.quote_string(file)
        query += " INTO TABLE %s " % table
        
        return self.mysql_query(query)
    
    
    def close(self):
        self.mysql_close()
        return super(MySQLSink, self).close()

     
    @staticmethod
    def new_client_connection(graphname, host = False, port = False ):
        return MySQLGlue( ClientTransport(graphname, host, port) )
    

    @staticmethod
    def new_slave_connection(command, cwd = None, env = None ):
        return MySQLGlue( SlaveTransport(command, cwd, env) )
    
    
    def dump_query (self, sql ):
        print "*** %s ***" % sql
        
        res = self.mysql_query( sql )
        if ( not res ): return False
        
        return self.dump_result( res )
    
    
    def dump_result (self, res ):
        keys = None
        c = 0
        
        print ""
        while True:
            row = self.mysql_fetch_assoc( res )
            if not row: break
            
            if keys is None :
                keys = array_keys( row )

                s = ""
                for k in keys:
                    s += k
                    s += "\t"
                
                
                print s
            
            s = ""
            for k, v in row:
                    s += v
                    s += "\t"
            
            print s
            c += 1
        
        
        print "-----------------------------"
        print "%i rows" % c
        
        return c
    

