# This file is subject to the terms and conditions defined in
# file 'LICENSE', which is part of this source code package.
#       Copyright (c) 2010 SKR Farms (P) LTD.

# -*- coding: utf-8 -*-

try :
    import cjson as json
    class JSON( object ):
        def __init__( self ):
            self.encode = json.encode
            self.decode = json.decode
except :
    import json
    class JSON( object ):
        def __init__( self ):
            self.encode = json.JSONEncoder().encode
            self.decode = json.JSONDecoder().decode
