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

class NoResultSetError(Exception):
    pass

class DbConnection(object):
    def __init__(self, connection_string):
        """Initialize the connection string and connection"""
        self.connection_string = connection_string
        self.conn = None

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
        
    def _query_key_maker(self, *args, **kwargs):
        """Generate hashable keys for the query method

        The paramaters are scrubbed, turning them into a tuple
        or tuple of tuples, as needed
        """
        arg_copy = list(args)
        query = remove_ws(arg_copy.pop(0)).lower()
        tupled_params = tuplify(arg_copy)
        kwarg_copy = tuple([(k,v) for k,v in kwargs.iteritems()])
        return (query, tupled_params, kwarg_copy)
        
    def _submit(self, query, params, headers=False, results=True,
                dictify=False):
        """Perform the actual query execution with the given
        pyodbc connection, query string, and query parameters"""
        if not self.conn:
            self.conn = pyodbc.connect(self.connection_string,
                                       autocommit=True)
            self.conn.add_output_converter(pyodbc.SQL_BINARY, bytestohex)
        cursor = self.conn.execute(query, params)

        if results:
            # Jump resultsets until data is found
            while not cursor.description and cursor.nextset():
                pass
            if not cursor.description:
                logger.debug('No resultset for query %s' % query)
                raise NoResultSetError()

            results = []
            if dictify:
                cols = [column[0] for column in cursor.description]
                for row in cursor.fetchall():
                    results.append(dict(zip(cols, row)))
            elif headers:
                results.append([column[0] for column in cursor.description])
                for row in cursor.fetchall():
                    results.append(tuple(row))
            else:
                for row in cursor.fetchall():
                    results.append(tuple(row))
            cursor.close()
            return results
        else:
            return ()

    @memoize(_query_key_maker)
    def query(self, query, *params, **kwargs):
        """Execute an SQL query

        Given a select query, query params, a choice of getting
        column headers, and of getting results at all,
        translate the params to bytearrays as necessary and tuplify
        them.  
        
        Also, modify the query string to account for uses of
        the IN clause in the query, matching the number ofparameters
        (ie, question marks) in the query string to the number of
        elements in the associated param.

        Return a tuple or dict with the query results.
        """
        headers = kwargs.get('headers', False)
        results = kwargs.get('results', True)
        dictify = kwargs.get('dictify', False)
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

        logger.debug('Running sql: %s, %s' % (new_query, repr(params)))
        results = self._submit(new_query, new_params, headers=headers,
                               results=results, dictify=dictify)
        return results
