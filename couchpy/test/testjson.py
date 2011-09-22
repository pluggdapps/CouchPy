#!/usr/bin/env python

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


import json, time, cjson
from   random import choice
from   copy   import deepcopy

PROFCOUNT = 10

# Basic types
_true  = True
_false = False
_str   = 'hello world' * 100
_uni   = u'hello world' * 100
_int   = 0x12345678
_long  = 0x1234567812345678
_float = 2.3
_none  = None

# Composite types
_list  = [ _true, _false, _str, _uni, _int, _long, _float, _none ]
_tuple = ( _true, _false, _str, _uni, _int, _long, _float, _none )
_dict  = {
    '_true' : _true,
    '_false' : _false,
    '_str' : _str,
    '_uni' : _uni,
    '_int' : _int,
    '_long' : _long,
    '_float' : _float,
    '_none' : _none,
}

# construct object
def construct( obj, elem ):
    if isinstance(elem, list) :
        elem.append( obj )
    elif isinstance(elem, tuple) :
        elem = tuple( list(elem) + [obj] )
    elif isinstance(elem, dict) :
        elem[ str(id(obj)) ] = obj
    return elem

elem = deepcopy( choice([_list, _tuple, _dict]) )
for i in range(PROFCOUNT) :
    elem = construct( elem, deepcopy( choice([_list, _tuple, _dict]) ))


#---- Standard library json
stdenc = json.JSONEncoder()
stddec = json.JSONDecoder()

st = time.time()
[ stdenc.encode(elem) for i in range(PROFCOUNT) ]
print "STDLIB Encoding time taken %s " % ( (time.time() - st) / PROFCOUNT )

jstext = stdenc.encode( elem )

st = time.time()
[ stddec.decode(jstext) for i in range(PROFCOUNT) ]
print "STDLIB Decoding time taken %s " % ( (time.time() - st) / PROFCOUNT )


#----- JSON C library
st = time.time()
[ cjson.encode(elem) for i in range(PROFCOUNT) ]
print "CJSON Encoding time taken %s " % ( (time.time() - st) / PROFCOUNT )

jstext = stdenc.encode( elem )

st = time.time()
[ cjson.decode(jstext) for i in range(PROFCOUNT) ]
print "CJSON Decoding time taken %s " % ( (time.time() - st) / PROFCOUNT )
