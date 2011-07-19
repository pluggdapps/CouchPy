from    client      import Client
#import database
#import doc
#import designdoc
#import attachment
#import query
#import view
#import httpc
#import httperror
#import rest

__version__ = '0.1'

class CouchPyError( Exception ) :
    """Raise an error because of failure detected in CouchPy library code."""

class AuthSession( dict ) :

    def __init__( self, *args, **kwargs ) :
        dict.__init__( self, *args, **kwargs )

    #---- properties

    ok = property( lambda self : self.get('ok', None) )
    userCtx = property( lambda self : self.get('userCtx', None) )
    info = property( lambda self : self.get('info', None) )
