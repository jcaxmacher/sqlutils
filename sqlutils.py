import re
import logging
import pyodbc
import inspect
from exceptions import Exception
from funcutils import *

logger = logging.getLogger('sqlutils')
    
def is_hex_string(s):
    """Test for "hex"-ness of a string"""
    return (isinstance(s, str) or isinstance(s, unicode)) \
           and len(s) > 2 and s[:2] == '0x' and len(s) % 2 == 0

def hextobytes(hs):
    """Convert a hex string to bytearray

    Given a string of an even length, containing
    hexidecimal characters (e.g., 0xAB34F1), convert
    it to an array of bytes (chopping of the 0x)
    """
    try:
        return bytearray([int(c, 16) for c in chunks(hs[2:], 2)])
    except:
        return hs

def bytestohex(bs):
    """Convert bytearray to hex string  

    Given a bytearray, convert it to a string of
    an even length, containing the associated
    hexidecimal characters for each byte
    """
    try:
        hs = ["{0:0>2}".format(hex(b)[2:].upper()) for b in bs]
        return '0x' + ''.join(hs)
    except:
        return bs

def hex_tupler(l):
    """Tuplify a list, converting hex strings into bytes"""
    return tuplify(l, lambda i: hextobytes(i) \
                      if is_hex_string(i) \
                      else i)
    
class SqlQueryParamsError(Exception):
    pass

class DbConnections(object):
    def __init__(self, conns={}, **kwargs):
        """Initialize function registry, db connection registry,
        and the query results cache, then make the db connections
        using the supplied connection strings in conns or kwargs"""
        self.funcs, self.conns = {}, {}
        self.add(**conns)
        self.add(**kwargs)

    def add(self, **kwargs):
        """Make db connections and store them by short name in a dict"""
        for k, conn_string in kwargs.iteritems():
            self.conns[k] = pyodbc.connect(conn_string, autocommit=True)
            self.conns[k].add_output_converter(pyodbc.SQL_BINARY, bytestohex)

    def _questionmarks(self, l):
        """Create a string of question marks separated by commas"""
        if is_seq(l):
            length = len(l) if len(l) > 0 else 1
            return '(%s)' % ','.join(length * '?')
        else:
            return '(?)'

    def _query_in_location(self, query):
        """Get the parameter locations of IN components of an sql query"""
        tokens = query.split()
        in_params = []
        for i in xrange(len(tokens)):
            if tokens[i] == '(?)':
                in_params.append((i, True))
            elif tokens[i] == '?':
                in_params.append((i, False))
        return (tokens, in_params)

    def _reparamaterize_query(self, query, params):
        """Add question marks as needed to support the use
        of the IN clause in the query"""
        tokens, ins = self._query_in_location(query)
        for i, v in enumerate(ins):
            if v[1]:
                tokens[v[0]] = self._questionmarks(params[i])
        return ' '.join(tokens)
        
    def _run_key_maker(self, *args, **kwargs):
        """Generate hashable keys for the run method

        Gets the database connection name for the function
        calling the run method and makes a tuple of that
        along with the query string and parameters.

        The paramaters are scrubbed, turning them into a tuple
        or tuple of tuples, as needed
        """
        mod_name, func = caller_info(levels_down=2)
        conn_name = self.funcs.get('%s.%s' % (mod_name, func))
        conn = self.conns.get(conn_name)
        arg_copy = list(args)
        query = remove_ws(arg_copy.pop(0)).lower()
        tupled_params = tuplify(arg_copy)
        return ((conn_name, query, tupled_params), {'conn': conn})
        
    def _run(self, conn, query, params, headers=False):
        """Perform the actual query execution with the given
        pyodbc connection, query string, and query parameters"""
        cursor = conn.execute(query, params)
        results = []
        if headers:
            results.append([column[0] for column in cursor.description])
        for row in cursor.fetchall():
            results.append(tuple(row))
        cursor.close()
        return results

    @memoize(_run_key_maker)
    def run(self, query, *params, **kwargs):
        """Execute an SQL query

        Given a select query, query params and a pyodbc connection,
        translate the params to bytearrays as necessary and tuplify
        them.  
        
        Also, modify the query string to account for uses of
        the IN clause in the query, matching the number ofparameters
        (ie, question marks) in the query string to the number of
        elements in the associated param.

        Return a tuple of tuples with the query results, where the
        first element of the tuple is a tuple of the returned column
        names.
        """
        conn = kwargs.get('conn')
        headers = kwargs.get('headers', False)
        new_query = self._reparamaterize_query(query, params)
        new_params = pipe(params, [
            hex_tupler,
            flatten,
            list
        ])
        if not new_query.count('?') == len(new_params):
            logger.debug('Parameters supplied do not match query.'
                         ' Raising exception.')
            raise SqlQueryParamsError()

        if not conn:
            logger.debug('No connection chosen. Would have run sql: '
                         '%s, %s' % (new_query, repr(params)))
            return ()
        else:
            logger.debug('Running sql: %s, %s' % (new_query, repr(params)))
            results = self._run(conn, new_query, new_params, headers=headers)
            return results

    def use(self, c):
        """Takes a db connection name and returns a decorator

        Given the shortname of a db connection that has been
        registered with the DbConnection object, create a decorator
        function that links that connection name with the given
        function in the DbConnections.funcs dict
        """
        def dec(f):
            """Takes a function and returns it unchanged

            Takes a function and registers the function details
            along with the associated db connection name in 
            the DbConnections.funcs dict, returning the function
            without modification
            """
            _func = '%s.%s' % (f.__module__, f.func_name)
            self.funcs[_func] = c
            return f
        return dec
