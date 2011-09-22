# CouchPy Couchdb data-modeling for CouchDB database management systems
#   Copyright (C) 2011  SKR Farms (P) LTD
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-

__version__ = '0.1dev'

hdr_acceptjs  = { 'Accept' : 'application/json' }
hdr_accepttxtplain = { 'Accept' : 'text/plain' }
hdr_acceptany      = { 'Accept' : '*/*' }
hdr_ctypejs   = { 'Content-Type' : 'application/json' }
hdr_ctypeform = { 'Content-Type' : 'application/x-www-form-urlencodeddata' }

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
