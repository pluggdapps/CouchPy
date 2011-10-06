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

"""Encoding python object to JSON text and decoding JSON text to python object
using the fastest available implementation. Searches for following
implementation in the listed order of priority.

* python-cjson C implementation of JSON encoder and decoder
* json JSON encoder and decoder from python standard-library
"""

try :
    import cjson
    class JSON( object ):
        def __init__( self ):
            self.encode = cjson.encode
            self.decode = cjson.decode
except :
    import json
    class JSON( object ):
        def __init__( self ):
            self.encode = json.JSONEncoder().encode
            self.decode = json.JSONDecoder().decode
            

class ConfigDict( dict ):
    def __init__( self, *args, **kwargs ):
        self._spec = {}
        dict.__init__( self, *args, **kwargs )

    def __setitem__( self, name, value ):
        self._spec[name] = value
        return dict.__setitem__( self, name, value['default'] )

    def specifications( self ):
        return self._spec
