# This file is subject to the terms and conditions defined in
# file 'LICENSE', which is part of this source code package.
#       Copyright (c) 2011 SKR Farms (P) LTD.

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
