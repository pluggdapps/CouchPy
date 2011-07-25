"""Query class to contruct, derive CoucDB database queries. Queries are
constructed on views or for database/_all_docs using a list of key,value pairs.

Create Query objects

>>> Query({ 'startkey' : '10', 'limit' : 2 })
>>> q.query()                                   # URL-query string
?startkey=10&limit=2

Manipulate Query objects

>>> q = Query( startkey='10', limit=2 )         # Same as before.
>>> q['startkey'] = '20'                        # Update parameters
>>> q.update({ 'endkey' : '40' })
?startkey=20&endkey=40&limit=2
>>> q['startkey']
20

Query factories

>>> q = Query( startkey='10', limit=2 )         # Same as before.
>>> q1 = q( endkey='40' )   # Create a new query based on the previous one

"""

import logging
from   copy             import deepcopy

log = logging.getLogger( __name__ )

class Query( object ) :
    def __init__( self, params={}, **q ) :
        """Instantiate a query object with a dictionary of key,value pairs
        passed as a key-word argument ``params``. Alternately, key,value pairs
        can be passed as key-word arguments directly.

        >>> Query({ 'startkey' : '10', 'limit' : 2 })
        >>> Query( startkey='10', limit=2 )
        """
        self._params = deepcopy( params )
        self._params.update( q )

    def __call__( self, params={}, **q ) :
        """Derive a new query object based on this objec's current parameter
        list. Pass the new query paramters as a dictionary to ``params``
        key-word argument, else the key,value pairs can be passed directly
        as key-word arguments.
        """
        d = deepcopy( self._params )
        d.update( params )
        d.update( q )
        return Query( **d )

    def __getitem__( self, key ) :
        """Get the value for query parameter ``key``"""
        return self._params.get( key, None )

    def __setitem__( self, key, value ) :
        """Set the ``value`` for query parameter ``key``"""
        return self._params.update({ key : value })

    def __delitem__( self, key ) :
        """Delete the (key,value) pair from the query's parameter list""" 
        return self._params.pop( key )

    def __iter__( self ) :
        """Iterate on query parameters as key, value pairs"""
        return iter( self._params.items() )

    def __repr__( self ) :
        return '<%s %r>' % (type(self).__name__, self.query())

    def items( self ) :
        """Return a list of key,value tuple"""
        return self._params.items()

    def update( self, params={}, **q ) :
        """Update this query object with new a dictionary of parameters,
        passes as key-word argument ``params``, or otherwise the key,value
        pairs can themselves be passed as key-word arguments"""
        self._params.update( params )
        self._params.update( q )
        return self

    def query( self ) : 
        """Construct url query string from key,value pairs and return the same.
        """
        r = '&'.join([ '%s=%s' % (k,v) for k,v in self._params.items() ])
        return '?%s' % r if r else ''

