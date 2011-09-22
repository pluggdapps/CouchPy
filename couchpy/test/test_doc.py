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

import sys, os, pprint, logging
from   os.path      import join, abspath, basename, splitext
from   copy         import deepcopy
from   random       import choice

import couchpy
from   couchpy.client    import Client
from   couchpy.database  import Database
from   couchpy.doc       import Document, LocalDocument, ImmutableDocument, Attachment
from   couchpy.httperror import *

log = logging.getLogger( __name__ )
files = map(
          lambda f : abspath(f),
          filter( lambda f : splitext(f)[1]=='.py', os.listdir('.') )
        )

sampledoc = {
    '_id'    : 'joe',
    'name'   : 'joe',
    'dob'    : '17/08/70',
    'father' : 'basha',
    'mother' : 'Lui',
}

def test_doc( url ) :
    c = Client( url=url, debug=True )
    ca = Client( url=url, debug=True )
    ca.login( 'pratap', 'pratap' )

    [ db.delete() for db in ca.databases ]
    dba = ca.put( 'testdb' )
    db  = c.Database( 'testdb' )

    print "Testing document post() method ..."
    doc = db.Document( sampledoc )
    assert doc._id not in db
    assert db.Document( sampledoc ) is doc
    doc.post()
    assert db.Document( sampledoc ) is doc

    print "Testing __getattr__, __setattr_, __getitem__ and __setitem__ operation ..."
    doc = db.Document( 'joe' ).fetch()
    assert isinstance( doc._x_hthdrs, dict )
    #----
    try : doc._x_notexist
    except : pass
    else : assert False
    #----
    assert doc.name == 'joe'
    assert doc._rev.startswith( '1-' )
    try : doc.notexits 
    except AttributeError : pass
    else : assert False
    #----
    assert doc['name'] == 'joe'
    assert doc['_rev'].startswith( '1-' )
    try : doc['notexists']
    except : pass
    else : assert False
    #----
    doc.friend = 'akbar'
    doc.put()
    doc = db.Document( 'joe' ).fetch()
    assert doc.friend == 'akbar'

    print "Testing __delitem__ operation ..."
    assert doc.friend == 'akbar'
    del doc['friend']
    doc.put()
    doc = doc.fetch()
    try : doc.friend
    except : pass
    else : assert False
    
    print "Testing _x_dirty and fetch(), put() methods ..."
    doc = db.Document( 'joe' ).fetch()
    doc.update( friend='akbar', _x_dirty=False )
    doc.fetch()
    assert 'friend' not in doc
    doc.update( friend='akbar' )
    doc.put().fetch()
    assert 'friend' in doc
    #----
    del doc['friend']
    try : doc.fetch()
    except : pass
    else : assert False
    doc.put().fetch()
    assert 'friend' not in doc

    print "Testing copy() method ..."
    doc1 = doc.copy( 'joecopy' )
    assert 'joecopy' in db 
    
    print "Testing delete() method ..."
    doc.delete()
    assert 'joe' not in db
    doc = doc( rev=doc['_rev'] ).fetch()
    assert isinstance( doc, ImmutableDocument )
    assert doc._deleted == True

    print "Testing Attachments ..."
    if c.version() == '1.0.1' :
        return 
    sampledoc1 = deepcopy( sampledoc )
    sampledoc1.update( _id='joeattach' )
    doc = db.Document( sampledoc1 )
    f1 = choice( files ) ; files.remove( f1 )
    doc.attach( f1 )
    f2 = choice( files ) ; files.remove( f2 )
    doc.attach( f2 )
    doc.post()
    attachfiles = sorted( [f1, f2], key=lambda x : basename(x) )
    #----
    doc = db.Document( 'joeattach' ).fetch()
    attachs = sorted( doc.attachments(), key=lambda x : x.filename )
    assert len(attachs) == 2
    assert all([ isinstance(x, Attachment) for x in attachs ])
    assert attachs == attachfiles
    assert [ x.data for x in attachs ] == [ None, None ]
    [ x.fetch() for x in attachs ]
    assert attachs[0].data == open( attachfiles[0] ).read()
    assert attachs[1].data == open( attachfiles[2] ).read()
    attachs[0].delete()
    attachs[1].delete()
    doc = db.Document( 'joeattach' ).fetch()
    assert doc.attachments() == []
    #----
    f3 = choice( files )
    doc.Attachment( filepath=f3 ).put()
    doc = db.Document( 'joeattach' ).fetch()
    attachs = sorted( doc.attachments(), key=lambda x : x.filename )
    assert len(attachs) == 1
    assert attachs[0].data == open( f3 ).read()

def test_localdoc( url ):
    c = Client( url=url, debug=True )
    ca = Client( url=url, debug=True )
    ca.login( 'pratap', 'pratap' )

    [ db.delete() for db in ca.databases ]
    dba = ca.put( 'testdb' )
    db  = c.Database( 'testdb' )

    print "Testing local document post(), fetch(), put(), delete() methods ..."
    ldoc = db.LocalDocument( sampledoc )
    ldoc.put()
    ldoc = db.LocalDocument( 'joe' ).fetch()
    assert ldoc.name == 'joe'
    assert ldoc.father == 'basha'
    #----
    ldoc.friend = 'akbar'
    ldoc.put()
    ldoc = db.LocalDocument( 'joe' ).fetch()
    assert ldoc.friend == 'akbar'
    del ldoc['friend']
    ldoc.put()
    ldoc = db.LocalDocument( 'joe' ).fetch()
    assert 'friend' not in ldoc
    
    print "Testing local document copy() method ..."
    ldoc1 = ldoc.copy( '_local/joecopy' )
    ldoc1 = db.LocalDocument( 'joecopy' ).fetch()
    assert ldoc1.name == 'joe'
    
    print "Testing local document delete() method ..."
    ldoc.delete()
    try : ldoc = db.LocalDocument( 'joe' ).fetch()
    except : pass
    else : assert False


def test_statemachine( url ):
    from  couchpy.doc import ST_ACTIVE_POST, ST_ACTIVE_INVALID, ST_ACTIVE_VALID, \
                             ST_ACTIVE_DIRTY

    print "Testing statemachine ..."

    c = Client( url=url, debug=True )
    ca = Client( url=url, debug=True )
    ca.login( 'pratap', 'pratap' )

    [ db.delete() for db in ca.databases ]
    dba = ca.put( 'testdb' )
    db  = c.Database( 'testdb' )

    doc = db.Document( sampledoc )
    assert doc._x_state == ST_ACTIVE_POST
    doc.father = 'akbar'
    assert doc._x_state == ST_ACTIVE_POST
    try : doc.put()
    except : pass
    else : assert False
    #----
    doc.post()
    assert doc._x_state == ST_ACTIVE_VALID
    doc.fetch()
    assert doc._x_state == ST_ACTIVE_VALID
    try : doc.put()
    except : pass
    else : assert False
    try : doc.post()
    except : pass
    else : assert False
    #----
    doc.father = 'akbar'
    assert doc._x_state == ST_ACTIVE_DIRTY
    doc.father = 'birbal'
    try : doc.post()
    except : pass
    else : assert False
    try : doc.fetch()
    except : pass
    else : assert False
    doc.put()
    doc = db.Document( 'joe' ).fetch()
    assert doc.father == 'birbal'
    assert doc._x_state == ST_ACTIVE_VALID


if __name__ == '__main__' :
    url = 'http://localhost:5984/'
    c = Client( url=url, debug=True )
    print 'CouchDB version %s' % c.version()
    test_doc( url )
    test_localdoc( url )
    test_statemachine( url )
