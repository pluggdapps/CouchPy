# This file is subject to the terms and conditions defined in
# file 'LICENSE', which is part of this source code package.
#       Copyright (c) 2011 SKR Farms (P) LTD.

# -*- coding: utf-8 -*-

import logging
from   couchpy.utils     import ConfigDict

__version__ = '0.1dev'

hdr_acceptjs  = { 'Accept' : 'application/json' }
hdr_accepttxtplain = { 'Accept' : 'text/plain' }
hdr_acceptany      = { 'Accept' : '*/*' }
hdr_ctypejs   = { 'Content-Type' : 'application/json' }
hdr_ctypeform = { 'Content-Type' : 'application/x-www-form-urlencodeddata' }

defconfig = ConfigDict()
defconfig['realm']             = {
    'default' : 'http://localhost:5984',
    'types'   : (str,),
    'help'    : "CouchDB HTTP-API URL. A single couchdb instance is considered "
                "as a realm."
}
defconfig['userdb.name']       = {
    'default' : '_users',
    'types'   : (str,),
    'help'    : "CouchDB user database for authentication and other user "
                "administration"
}
defconfig['couch_httpd_auth.timeout']  = {
    'default' : 10000000,
    'types'   : (int,),
    'help'    : "How long authenticated session should be preserved before "
                "timing-out."
}
defconfig['uuids.algorithm']   = {
    'default' : 'utc_random',
    'types'   : (str,),
    'help'    : "Algorithm to generate universally unique identifiers for "
                "documents."
}
defconfig['cookie.name']       =  {
    'default' : 'AuthSession',
    'types'   : (str,),
    'help'    : "Cookie name to store user-authentacation. The name must be "
                "same as that used by couchdb API."
}


class CouchPyError( Exception ) :
    """Raise an error because of failure detected in CouchPy library code."""

class AuthSession( dict ) :

    def __init__( self, *args, **kwargs ) :
        dict.__init__( self, *args, **kwargs )

    #---- properties

    ok = property( lambda self : self.get('ok', None) )
    userCtx = property( lambda self : self.get('userCtx', None) )
    info = property( lambda self : self.get('info', None) )


class BaseIterator( object ):

    def __init__( self, values=None, *args, **kwargs ):
        self.values = values
        self.offset = kwargs.get( 'offset', None )
        self.limit = kwargs.get( 'limit', 100 )
        self.fetchfn = kwargs.get( 'fetchfn', None )

    def __iter__( self ):
        return self

    def next( self ):
        if self.values :
            return self.values.pop(0)
        self.values, self.offset = self.getvalues( self.offset, self.limit )
        if self.values :
            return self.values.pop(0)
        raise StopIteration

    def getvalues( self, offset, limit ):
        if self.fetchvalues :
            return self.fetchvalues(offset, limit)
        else :
            return (None, None) # values, offset
