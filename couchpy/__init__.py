#import client
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
