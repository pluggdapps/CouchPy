#!/usr/bin/env python

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
