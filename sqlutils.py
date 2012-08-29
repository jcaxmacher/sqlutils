import re
import logging
import pyodbc
import inspect
from functools import wraps

logger = logging.getLogger('sqlutils')

def caller_info():
    """Return module and function name of function two levels
    down the stack, so- the caller of whichever function called
    this one"""
    # inspect stack for caller module and function
    func, mod, mod_name = None, None
    caller = inspect.stack()[2]
    try:
        func = caller[3]
        mod = inspect.getmodule(caller[0])
        mod_name = getattr(mod, '__name__', '__main__')
    finally:
        del caller
        del mod
    return mod_name, func

def flatten(l):
    """Take a heterogeneous list which may contain sublists
    and flatten them into a single stream of values"""
    for i in l:
        if is_seq(i):
            for j in flatten(i):
                yield j
        else:
            yield i

def tuplify(l, modifier=None):
    """Convert lists and sublists to tuples
    with optional modifier of each element"""
    new_l = []
    for i in new_l:
        if is_seq(i):
            new_l.append(tuplify(i))
        elif modifier:
            new_l.append(modifier(i))
        else:
            new_l.append(i)
    return tuple(new_l)

def qmarks(l):
    """Create a string of question marks separated by commas"""
    if is_seq(l):
        return '(%s)' % ','.join(len(l) * '?')
    else:
        return '(?)'

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
    if getattr(item, '__iter__', None) and not isinstance(item, bytearray):
        return True
    else:
        return False

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

    def _run(self, conn, query, params):
        """Perform the actual query execution with the given
        pyodbc connection, query string, and query parameters"""
        cursor = conn.execute(query, params)
        results = []
        results.append([column[0] for column in cursor.description])
        for row in cursor.fetchall():
            results.append(row)
        cursor.close()
        return results

    def run(self, query, *params):
        """Execute an SQL query

        Given a select query, query params and a connection
        string, execute the query and return the results as
        a list of dict, where the dict keys are the column names
        """
        mod_name, func = caller_info()

        # get connection for for caller
        conn_name = self.funcs.get('%s.%s' % (mod_name, func))
        conn = self.conns.get(conn_name)

        # make params hashable, if possible
        tupled_params = tuplify(params)
        cache_key = (conn_name, query, tupled_params)

        # return cached results if present
        cached_results = self.cache.get(cache_key)
        if cached_results:
            logger.debug('Getting cached result: %s, %s' % (remove_ws(query),
                                                            repr(params)))
            return cached_results

        # translate hex params to bytearrays
        hexed_params = tuplify(tupled_params,
                               lambda i: b(i) if is_hex_string(i) else i)

        # get tokenized query and location of INs in query
        tokens, ins = query_in_location(query)
        for i, v in enumerate(ins):
            if v[1]:
                tokens[v[0]] = qmarks(hexed_params[i])

        new_query = ' '.join(tokens)
        flattened_params = list(flatten(hexed_params))
            
        if not conn:
            logger.debug('No connection chosen.  '
                         'Would have run sql: %s, %s' % (new_query,
                                                repr(params)))
            return ()
        else:
            logger.debug('Running sql: %s, %s' % (new_query,
                                                repr(params)))
            results = self._run(conn, new_query, flattened_params)
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
