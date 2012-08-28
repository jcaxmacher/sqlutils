import re
import logging
import pyodbc
import inspect
from functools import wraps

logger = logging.getLogger('sqlutils')

def flatten(l):
    for i in l:
        if is_seq(i) and not isinstance(i, bytearray):
            for j in flatten(i):
                yield j
        else:
            yield i

def qmarks(l):
    """Create a string of question marks separated by commas"""
    return '(%s)' % ','.join(len(l) * '?')

def query_in_location(query):
    """Get the parameter locations of IN components of an sql query"""
    tokens = query.split()
    in_params = []
    for i in xrange(len(tokens)):
        if tokens[i] == '(?)':
            in_params.append((i, True))
        elif tokens[i] == '?':
            in_params.append((i, False))
    return (tokens, in_params)

def is_seq(item):
    """Check for sequences"""
    return True if getattr(item, '__iter__', None) else False

def remove_ws(query):
    """Remove excessive whitespace from string"""
    return re.sub(r'\s+', r' ', query.replace('\n','').strip())
    
def is_hex_string(s):
    """Test for "hex"-ness of a string"""
    return isinstance(s, str) and len(s) > 2 and s[:2] == '0x' \
        and len(s) % 2 == 0

def chunks(l, n):
    """Yield successive n-sized chunks from l."""
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def b(hs):
    """Convert a hex string to bytearray

    Given a string of an even length, containing
    hexidecimal characters (e.g., 0xAB34F1), convert
    it to an array of bytes (chopping of the 0x)
    """
    try:
        return bytearray([int(c, 16) for c in chunks(hs[2:], 2)])
    except:
        return hs

def h(bs):
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

class DbConnections(object):
    def __init__(self, conns={}, **kwargs):
        """Initialize function registry, db connection registry,
        and the query results cache, then make the db connections
        using the supplied connection strings in conns or kwargs"""
        self.funcs, self.conns, self.cache = {}, {}, {}
        self.add(**conns)
        self.add(**kwargs)

    def add(self, **kwargs):
        """Make db connections and store them by short name in a dict"""
        for k, conn_string in kwargs.iteritems():
            self.conns[k] = pyodbc.connect(conn_string, autocommit=True)
            self.conns[k].add_output_converter(pyodbc.SQL_BINARY, h)
            
    def _process_params(self, params):
        new_params = []
        if is_seq(params):
            for p in params:
                new_params.append(p if not is_seq(p) else tuple(p))
        else:
            new_params.append(params)
        return tuple(new_params)

    def run(self, query, params=()):
        """Execute an SQL query

        Given a select query, query params and a connection
        string, execute the query and return the results as
        a list of dict, where the dict keys are the column names
        """
        # inspect stack for caller module and function
        func, mod = None, None
        caller = inspect.stack()[1]
        try:
            func = caller[3]
            mod = inspect.getmodule(caller[0])
            mod_name = getattr(mod, '__name__', '__main__')
        finally:
            del caller
            del mod

        # get connection ifor for caller
        conn_name = self.funcs.get('%s.%s' % (mod_name, func))
        conn = self.conns.get(conn_name)

        new_params = self._process_params(params)
        cache_key = (conn_name, query, new_params)

        # return cached results if present
        cached_results = self.cache.get(cache_key)
        if cached_results:
            logger.debug('Getting cached result: %s, %s' % (remove_ws(query),
                                                            repr(params)))
            return cached_results

        # translate hex params to bytearrays
        hexed_params = []
        for p in new_params:
            if is_seq(p):
                sub_params = []
                for i in p:
                    sub_params.append(b(i) if is_hex_string(i) else i)
                hexed_params.append(tuple(sub_params))
            elif is_hex_string(p):
                hexed_params.append(b(p))
            else:
                hexed_params.append(p)
        new_params = tuple(hexed_params)

        # get tokenized query and location of INs in query
        tokens, ins = query_in_location(query)
        for i, v in enumerate(ins):
            if v[1]:
                tokens[v[0]] = qmarks(new_params[i])

        new_query = ' '.join(tokens)
            
        if not conn:
            logger.debug('No connection chosen.  '
                         'Would have run sql: %s, %s' % (new_query,
                                                repr(params)))
            return ()
        else:
            logger.debug('Running sql: %s, %s' % (new_query,
                                                repr(params)))
            cursor = conn.execute(new_query, list(flatten(new_params)))
            results = []
            results.append([column[0] for column in cursor.description])
            for row in cursor.fetchall():
                results.append(row)
            cursor.close()
            self.cache[cache_key] = results
            return results

    def choose(self, c):
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
