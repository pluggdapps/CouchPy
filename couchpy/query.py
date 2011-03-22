"""Query class to contruct, derive CoucDB database queries. A query consists
of key value parameter
"""

from   copy         import deepcopy

class Query( object ) :
    def __init__( self, params={}, **q ) :
        """Instantiate a query object with key, value parameters passed as a
        dictionary to `params` key-word argument, else the key,value pairs can
        themselves be passed as key-word arguments.
        """
        self._params = deepcopy( params )
        self._params.update( q )

    def __call__( self, params={}, **q ) :
        """Derive a new query object based on the current state of this
        object, which acts like a base query. Pass the new query paramters as
        a dictionary to `params` key-word argument, else the key,value pairs
        can themselves be passed as key-word arguments.
        """
        d = deepcopy( self._params )
        d.update( params )
        d.update( q )
        return Query( **d )

    def __getitem__( self, key ) :
        """Get the value for query parameter `key`"""
        return self._params.get( key )

    def __setitem__( self, key, value ) :
        """Set the `value` for query parameter `key`"""
        return self._params.update({ key : value })

    def __delitem__( self, key ) :
        """Delete a query parameter and its value, from this query object""" 
        return self._params.pop( key )

    def __iter__( self ) :
        """Iterate on query parameters as key, value pairs"""
        return iter( self._params.items() )

    def items( self ) :
        """Return a list of key,value tuples"""
        return self._params.items()

    def update( self, params={}, **q ) :
        """Update this query object with new parameters. Pass the new query
        parameters as a dictionary go `params` key-word argument, else the
        key,value pairs can themselves be passed as key-word arguments"""
        self._params.update( params )
        self._params.update( q )
        return self

    def query( self ) : 
        """Construct url query string from key,value pairs and return the
        same.
        """
        r = '&'.join([ '%s=%s' % k.v for k,v in self._params.items() ])
        return '?%s' % r if r else ''

