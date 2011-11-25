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
            

class ConfigItem( dict ):
    """Convenience class encapsulating config value description, which is a
    dictionary of following keys,

    ``default``,
        Default value for this settings parameter.
    ``types``,
        Comma separated value of valid types. Allowed types are str, unicode,
        basestring, int, long, bool, 'csv'. 'csv' is a custom defined.
    ``help``,
        Help string describing the purpose and scope of settings parameter.
    ``webconfig``,
        Boolean, specifying whether the settings parameter is configurable via
        web.
    """
    typestr = {
        str   : 'str', unicode : 'unicode', list : 'list', tuple : 'tuple',
        'csv' : 'csv', dict    : 'dict',    bool : 'bool', int   : 'int',
    }
    def _options( self ):
        opts = self.get( 'options', '' )
        return opts() if callable(opts) else opts

    # Compulsory fields
    default = property( lambda self : self['default'] )
    types   = property(
                lambda s : ', '.join([ s.typestr[k] for k in s['types'] ])
              )
    # Optional fields, mostly for rendering on user-agent.
    help = property( lambda self : self.get('help', '') )
    webconfig = property( lambda self : self.get('webconfig', True) )
    options = property( _options )


class ConfigDict( dict ):
    """Configuration class for package-default options. Along with the default
    options, it is possible to add help-text for each config-key, as a
    dictionary.

    The setting-value description will be aggregated as a dictionary under,
        self._spec
    """
    def __init__( self, *args, **kwargs ):
        self._spec = {}
        dict.__init__( self, *args, **kwargs )

    def __setitem__( self, name, value ):
        self._spec[name] = ConfigItem( value )
        return dict.__setitem__( self, name, value['default'] )

    def specifications( self ):
        return self._spec


