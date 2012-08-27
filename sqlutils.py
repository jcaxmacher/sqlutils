import re
import logging
import pyodbc
import inspect
from functools import wraps

logger = logging.getLogger('sqlutils')

def chunks(l, n):
    """Yield successive n-sized chunks from l.
    """
    for i in xrange(0, len(l), n):
        yield l[i:i+n]

def remove_ws(query):
    """Remove excessive whitespace from string"""
    return re.sub(r'\s+', r' ', query.replace('\n','').strip())
    
def is_hex_string(s):
    """Test for "hex"-ness of a string"""
    return isinstance(s, str) and len(s) > 2 and s[:2] == '0x' \
        and len(s) % s == 0

def b(hs):
    """Convert a hex string to bytearray

    Given a string of an even length, containing
    hexidecimal characters, convert it to an array
    of bytes
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
        self.current = None
        self.funcs = {}
        self.conns = {}
        self.cache = {}
        for k, conn_string in conns.iteritems():
            self.conns[k] = pyodbc.connect(conn_string, autocommit=True)
            self.conns[k].add_output_converter(pyodbc.SQL_BINARY, h)
        self.add(**kwargs)

    def add(self, **kwargs):
        for k, conn_string in kwargs.iteritems():
            self.conns[k] = pyodbc.connect(conn_string, autocommit=True)
            self.conns[k].add_output_converter(pyodbc.SQL_BINARY, h)
            
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
        finally:
            del caller

        # get connection ifor for caller
        conn_name = self.funcs.get('%s.%s' % (getattr(mod, '__name__',
                                                      '__main__'), func))
        conn = self.conns.get(conn_name)
        cache_key = (conn_name, query, params)

        # return cached results if present
        cached_results = self.cache.get(cache_key)
        if cached_results:
            logger.debug('Getting cached result: %s, %s' % (remove_ws(query),
                                                            repr(params)))
            return cached_results

        # translate hex params to bytearrays
        if isinstance(params, tuple):
            new_params = tuple([b(p) if is_hex_string(p) else p \
                                for p in params])
        elif is_hex_string(params):
            new_params = b(params)
        else:
            new_params = params

        if not conn:
            logger.debug('No connection chosen.  '
                         'Would have run sql: %s, %s' % (remove_ws(query),
                                                repr(params)))
            return ()
        else:
            logger.debug('Running sql: %s, %s' % (remove_ws(query),
                                                repr(params)))
            cursor = conn.execute(query, new_params)
            results = []
            results.append([column[0] for column in cursor.description])
            for row in cursor.fetchall():
                results.append(row)
            cursor.close()
            self.cache[cache_key] = results
            return results

    def choose(self, c):
        def dec(f):
            _func = '%s.%s' % (f.__module__, f.func_name)
            self.funcs[_func] = c
            @wraps(f)
            def wrapper(*args, **kwargs):
                return f()
            return wrapper
        return dec
