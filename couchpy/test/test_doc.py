#!/usr/bin/env python

# This file is subject to the terms and conditions defined in
# file 'LICENSE', which is part of this source code package.
#       Copyright (c) 2011 SKR Farms (P) LTD.

# -*- coding: utf-8 -*-

import sys, os, pprint, logging
from   os.path      import join, abspath, basename, splitext
from   copy         import deepcopy
from   random       import choice

import couchpy
from   couchpy.client    import Client
from   couchpy.database  import Database
from   couchpy.doc       import Document, LocalDocument, ImmutableDocument
from   couchpy.doc       import Attachment, Views, View, Query
from   couchpy.doc       import ST_ACTIVE_INVALID, ST_ACTIVE_VALID
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
designdoc = {
    '_id'   : '_design/example',
    'views' : {
        'example' : {
            "map"    : "function( doc ) { };",
            "reduce" : "function( keys, values, rereduce ) { };",
        },
    },
}

def test_statemachine( url ):
    from  couchpy.doc import ST_ACTIVE_POST, ST_ACTIVE_INVALID, ST_ACTIVE_VALID,\
                             ST_ACTIVE_DIRTY

    print "Testing statemachine ..."
    c = Client( url=url, )
    ca = Client( url=url, )
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


def test_doc( url ) :
    c = Client( url=url )
    ca = Client( url=url )
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

    print "Testing Document() instantiation ..."
    assert db.Document( 'joe' ).fetch() is doc
    doc = db.Document( 'joe' ).fetch()
    assert db.Document({ '_id': doc._id,  '_rev' : doc._rev }).fetch() is doc
    d = deepcopy( sampledoc )
    d['_id'] = 'joe1'
    try :
        db.Document( d ).fetch()
        assert False
    except :
        pass
    doc1 = db.Document(d).post()

    print "Testing Document delete() method ..."
    db.Document('joe1').fetch().delete()
    assert 'joe1' not in db
    doc1 = doc1( rev=doc1['_rev'] ).fetch()
    assert isinstance( doc1, ImmutableDocument )
    assert doc1._deleted == True


    print "Testing __getattr__, __setattr_, __getitem__ and __setitem__ operation ..."
    doc = db.Document( 'joe' ).fetch()
    assert isinstance( doc._x_hthdrs, dict )
    assert doc.name == 'joe'
    assert doc._rev.startswith( '1-' )
    doc.friend = 'akbar'
    doc.put()
    doc = db.Document( 'joe' ).fetch()
    assert doc.friend == 'akbar'
    assert doc['friend'] is doc.friend

    print "Testing Document fetch with revs ..."
    doc = db.Document( 'joe', revs='true' ).fetch()
    assert doc['_revisions']['start'] == 2
    assert len( doc['_revisions']['ids'] ) == 2

    print "Testing Document fetch with revs_info ..."
    doc = db.Document( 'joe', revs_info='true' ).fetch()
    _revs_info = doc['_revs_info']
    assert len( _revs_info ) == 2
    assert list(set( x['status'] for x in _revs_info )) == ['available']
    assert _revs_info[0]['rev'].startswith('2-')
    assert _revs_info[1]['rev'].startswith('1-')
    oldrev = _revs_info[1]['rev']

    print "Testing Document fetch for older revision ..."
    doc = db.Document( 'joe', rev=oldrev ).fetch()
    assert isinstance( doc, ImmutableDocument )
    assert 'friend' not in doc

    print "Testing __delitem__ operation ..."
    doc = db.Document( 'joe' ).fetch()
    assert doc.friend == 'akbar'
    del doc['friend']
    doc.put()
    doc = doc.fetch()
    assert 'friend' not in doc
    
    print "Testing changed(), is_dirty(), invalidate() methods ..."
    doc = db.Document( 'joe' ).fetch()
    doc.update( friend='akbar', _x_dirty=False )
    assert doc.is_dirty() == False
    doc.fetch()
    assert 'friend' not in doc
    #----
    doc.update( friend='akbar' )
    assert doc.is_dirty()
    doc.put().fetch()
    assert 'friend' in doc
    #----
    doc.changed()
    assert doc.is_dirty()
    doc.put()
    assert doc.is_dirty() == False
    #----
    assert doc._x_state == ST_ACTIVE_VALID
    doc.invalidate()
    assert doc._x_state == ST_ACTIVE_INVALID

    print "Testing head() method ..."
    doc = db.Document( 'joe' ).fetch()
    doc.head()['Etag'] == doc._rev

    print "Testing copy() method ..."
    doc1 = db.Document( doc.copy( 'joecopy' ) ).fetch()
    docrev = doc._rev
    assert 'joecopy' in db
    assert db.Document( 'joecopy' ) is doc1
    d = dict([ (k,v) for k,v in doc.items() if not k.startswith('_') ])
    d1 = dict([ (k,v) for k,v in doc1.items() if not k.startswith('_') ])
    assert d == d1
    doc1.copiedfrom = doc._id
    doc1.put()
    assert doc1._rev.startswith('2-')
    assert dict(doc.items()) != dict(doc1.items())
    doc1.copy( 'joe', asrev=doc._rev )
    doc.fetch()
    assert docrev != doc._rev
    assert doc._rev.startswith( str((int(docrev[0]) + 1)) )
    d = dict([ (k,v) for k,v in doc.items() if not k.startswith('_') ])
    d1 = dict([ (k,v) for k,v in doc1.items() if not k.startswith('_') ])
    assert d == d1

    print "Testing attach() method ..."
    sampledoc1 = deepcopy( sampledoc )
    sampledoc1.update( _id='joeattach' )
    doc = db.Document( sampledoc1 )
    f1 = choice( files ) ; files.remove( f1 )
    doc.attach( f1 )
    f2 = choice( files ) ; files.remove( f2 )
    doc.attach( f2 )
    doc.post()
    assert doc._rev.startswith( '1-' )
    attachfiles = sorted( [f1, f2], key=lambda x : basename(x) )
    print "Testing class Attachment methods ..."
    doc = db.Document( 'joeattach' ).fetch()
    attachs = sorted( doc.attachments(), key=lambda x : x.filename )
    assert len(attachs) == 2
    assert all([ isinstance(x, Attachment) for x in attachs ])
    assert [ x.filename for x in attachs ] == \
           map( lambda f : basename(f), attachfiles )
    assert [ x.data for x in attachs ] == [ None, None ]
    [ x.get() for x in attachs ]
    assert attachs[0].data == open( attachfiles[0] ).read()
    assert attachs[1].data == open( attachfiles[1] ).read()
    attachs[0].delete()
    attachs[1].delete()
    assert doc._rev.startswith( '3-' )

    print "Testing Attachment class ..."
    doc = db.Document( 'joeattach' ).fetch()
    assert doc.attachments() == []
    f3 = choice( files )
    doc.Attachment( filepath=f3 ).put()
    doc.fetch()
    attach = doc.Attachment( filepath=f3 ).get()
    assert attach.data == open( f3 ).read()

    
def test_localdoc( url ):
    c = Client( url=url )
    ca = Client( url=url )
    ca.login( 'pratap', 'pratap' )

    [ db.delete() for db in ca.databases ]
    dba = ca.put( 'testdb' )
    db  = c.Database( 'testdb' )

    db.Document( sampledoc ).post()

    print "Testing LocalDocument post(), fetch(), put(), delete() methods ..."
    ldoc = db.LocalDocument( sampledoc ).put()
    ldoc = db.LocalDocument( 'joe' ).fetch()
    assert ldoc.name == 'joe'
    assert ldoc.father == 'basha'

    print "Testing LocalDocument __getattr__, __setattr__ and __delitem__ methods ..."
    ldoc.friend = 'akbar'
    ldoc.put()
    ldoc = db.LocalDocument( 'joe' ).fetch()
    assert ldoc.friend == 'akbar'
    assert ldoc._rev.startswith('0-2')
    del ldoc['friend']
    ldoc.put()
    ldoc = db.LocalDocument( 'joe' ).fetch()
    assert 'friend' not in ldoc
    
    print "Testing local document copy() method ..."
    ldoc1 = ldoc.copy( '_local/joecopy' )
    ldocrev = ldoc._rev
    ldoc1 = db.LocalDocument( 'joecopy' ).fetch()
    d = dict([ (k,v) for k,v in ldoc.items() if not k.startswith('_') ])
    d1 = dict([ (k,v) for k,v in ldoc1.items() if not k.startswith('_') ])
    assert d == d1
    ldoc1.copiedfrom = ldoc._id
    ldoc1.put()
    assert ldoc1._rev.startswith('0-2')
    assert dict(ldoc.items()) != dict(ldoc1.items())
    #ldoc1.copy( 'joe', asrev=ldocrev )
    #ldoc.fetch()
    #assert ldocrev != ldoc._rev
    #assert ldoc._rev.startswith( str((int(ldocrev[0]) + 1)) )
    #d = dict([ (k,v) for k,v in ldoc.items() if not k.startswith('_') ])
    #d1 = dict([ (k,v) for k,v in ldoc1.items() if not k.startswith('_') ])
    #assert dict(ldoc.items()) == dict(ldoc1.items())
    
    print "Testing LocalDocument delete() method ..."
    ldoc.delete()
    try :
        ldoc = db.LocalDocument( 'joe' ).fetch()
        assert False
    except :
        pass


def test_immutdoc( url ) :
    c = Client( url=url )
    ca = Client( url=url )
    ca.login( 'pratap', 'pratap' )

    [ db.delete() for db in ca.databases ]
    dba = ca.put( 'testdb' )
    db  = c.Database( 'testdb' )

    print "Testing Immutable document post() method ..."
    doc = db.Document( sampledoc )
    assert doc._id not in db
    assert db.Document( sampledoc ) is doc
    doc.post()
    docrev = doc._rev
    doc.friend = 'akbar'
    doc.put()
    immdoc = db.Document( 'joe', rev=docrev ).fetch()
    assert isinstance( immdoc, ImmutableDocument )
    assert immdoc.name == 'joe'
    try :
        immdoc.name = 'joe'
        assert False
    except :
        pass


def test_designdoc( url ):
    print "Testing DesignDocument ..."
    c = Client( url=url, )
    ca = Client( url=url, )
    ca.login( 'pratap', 'pratap' )

    [ db.delete() for db in ca.databases ]
    dba = ca.put( 'testdb' )
    db  = c.Database( 'testdb' )

    print "Testing DesignDocument creation ..."
    ddoc = dba.DesignDocument( designdoc ).post()
    assert ddoc._rev.startswith( '1-' )
    d = dict( ddoc.items() )
    d.pop( '_rev' )
    assert d == designdoc

    print "Testing DesignDocument info() method ..."
    d = ddoc.info()
    assert d['name'] == 'example'
    assert 'view_index' in d
    keys = [ "compact_running", "updater_running", "language", "purge_seq",
             "waiting_commit", "waiting_clients", "signature", "update_seq",
             "disk_size" ]
    assert sorted( d['view_index'].keys() ) == sorted( keys )

    print "Testing DesignDocument views() method and Views class ..."
    views = ddoc.views()
    assert views.keys() == [ 'example' ]

    print "Testing View class ..."
    view = views['example']
    assert isinstance( view, View )
    assert view.doc == ddoc
    assert view.viewname == 'example'
    assert view.query == {}

    print "Testing Query class ..."
    qdict = { '_id' : 10, 'name' : 'xyz' }
    qdict1 = { '_id' : 10, 'name' : 'abc' }
    q = Query( **qdict ) if choice([0,1]) else Query( _q=qdict )
    q1 = Query( **qdict1 ) if choice([0,1]) else Query( _q=qdict1 )

    view1 = view( _q=q )
    assert view1.doc == ddoc
    assert view1.viewname == 'example'
    assert view1.query == qdict

    view2 = view( **q1 )
    assert view2.doc == ddoc
    assert view2.viewname == 'example'
    assert view2.query == qdict1


if __name__ == '__main__' :
    url = 'http://localhost:5984/'
    c = Client( url=url, )
    #logging.basicConfig( level=logging.INFO )
    print 'CouchDB version %s' % c.version()
    test_statemachine( url )
    print ''
    test_doc( url )
    print ''
    test_localdoc( url )
    print ''
    test_immutdoc( url )
    print ''
    test_designdoc( url )
