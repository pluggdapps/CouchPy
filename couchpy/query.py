from   copy         import deepcopy

class Query( object ) :
    def __init__( self, params={}, **q ) :
        self._params = deepcopy( params )
        self._params.update( q )

    def __call__( self, params={}, **q ) :
        d = deepcopy( self._params )
        d.update( params )
        d.update( q )
        return Query( **d )

    def __getitem__( self, key ) :

    def __setitem__( self, key, value ) :

    def __delitem__( self, key ) :

    def _value

    def params( self ) : 

    def items( self ) : 

