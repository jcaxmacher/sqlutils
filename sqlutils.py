"""
Basic pyodbc helper methods for simplifying sql statements
and caching query results.
"""
import logging
import pyodbc
from funcutils import memoize, flatten, tuplify, is_seq, remove_ws, \
    chunks, pipe

LOGGER = logging.getLogger('sqlutils')


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
    except ValueError:
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
    except TypeError:
        return bs


def hex_tupler(l):
    """Tuplify a list, converting hex strings into bytes"""
    return tuplify(l,
                   lambda i: hextobytes(i) if is_hex_string(i) else i)


def questionmarks(l):
    """Create a string of question marks separated by commas"""
    if is_seq(l):
        length = len(l) if len(l) > 0 else 1
        return '(%s)' % ','.join(length * '?')
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


def reparamaterize_query(query, params):
    """Add question marks as needed to support the use
    of the IN clause in the query"""
    tokens, ins = query_in_location(query)
    for i, v in enumerate(ins):
        if v[1]:
            tokens[v[0]] = questionmarks(params[i])
    return ' '.join(tokens)


class SqlQueryParamsError(Exception):
    """The parameters indicated in the query do not match up
    with the supplied arguments."""
    pass


class NoResultSetError(Exception):
    """No result sets were returned for the query."""
    pass


class DbConnection(object):
    """Class used to wrap pyodbc connections."""
    def __init__(self, connection_string):
        """Initialize the connection string and connection"""
        self.connection_string = connection_string
        self.conn = None

    def _query_key_maker(self, *args, **kwargs):
        """Generate hashable keys for the query method

        The paramaters are scrubbed, turning them into a tuple
        or tuple of tuples, as needed
        """
        arg_copy = list(args)
        query = remove_ws(arg_copy.pop(0)).lower()
        tupled_params = tuplify(arg_copy)
        kwarg_copy = tuple([(k, v) for k, v in kwargs.iteritems()])
        return (self.connection_string.lower(), query, tupled_params,
                kwarg_copy)

    def direct(self, query, params, **kwargs):
        """Perform the actual query execution with the given
        pyodbc connection, query string, and query parameters"""
        headers = kwargs.get('headers', False)
        results = kwargs.get('results', True)
        dictify = kwargs.get('dictify', False)
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
                LOGGER.debug('No resultset for query %s' % query)
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
            cursor.commit()
            cursor.close()
            return results
        else:
            cursor.commit()
            cursor.close()
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
        new_query = reparamaterize_query(query, params)
        new_params = pipe(params, [
            hex_tupler,
            flatten,
            list
        ])
        if not new_query.count('?') == len(new_params):
            LOGGER.debug('Parameters supplied do not match query.'
                         ' Raising exception.')
            raise SqlQueryParamsError()

        LOGGER.debug('Running sql: %s, %s' % (new_query, repr(params)))
        results = self.direct(new_query, new_params, headers=headers,
                              results=results, dictify=dictify)
        return results
