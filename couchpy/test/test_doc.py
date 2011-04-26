#! /usr/bin/env python

import sys, os, pprint, logging
from   os.path      import join, abspath, basename, splitext

import couchpy
from   couchpy.client    import Client
from   couchpy.database  import Database
from   couchpy.doc       import Document, LocalDocument
from   couchpy.httperror import *

# TODO :
#   1. hthdrs will be remembered. Test that !!
#   2. Attachments for local document

log = logging.getLogger( __name__ )
files = map(
          lambda f : abspath(f),
          filter( lambda f : splitext(f)[1]=='.py', os.listdir('.') )
        )

def _makedoc( **kwargs ) :
    return kwargs 

def test_create_document() :
    log.info( "Testing create document ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    # Basic create
    db = c.create( 'testdb' )
    doc = _makedoc( _id='testdoc', value=1000 )
    f = basename(files[0])
    doc = Document.create( db, doc, attachfiles=f )
    assert doc._id == 'testdoc'
    assert doc['value'] == 1000
    a = doc.attach( f )
    assert a.filename == f
    d, t = a.data()
    assert a.data() == (open( files[0] ).read(), a.content_type)

    # Test in batch mode.
    doc = _makedoc( _id='testdoc1', value=1001 )
    Document.create( db, doc, batch='ok', fetch=False )
    doc = _makedoc( _id='testdoc2', value=1002 )
    Document.create( db, doc, batch='ok' )
    databases = sorted([ _id for _id in db ]) 
    assert databases == [u'testdoc', u'testdoc1', u'testdoc2']

    [ c.delete(db.dbname) for db in c ]

def test_access_document() :
    log.info( "Testing access document ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]
    
    db = c.create( 'testdb' )
    refdoc1 = _makedoc( _id='testdoc1', value=1000 )
    Document.create( db, refdoc1 )

    # Fetch
    doc1 = Document( db, 'testdoc1' )
    assert ('_id', 'testdoc1') in doc1.items()
    assert ('value', 1000) in doc1.items()
    # Update
    doc1.update({ 'value' : 1001 })
    assert doc1.doc['value'] == 1001 and doc1._rev.startswith('2-')
    # Update again - empty
    doc1.update()
    assert doc1.doc['value'] == 1001 and doc1._rev.startswith('3-')
    # Fetch latest
    refdoc1 = Document( db, refdoc1 )
    assert refdoc1.doc == doc1.doc
    # Check .items() method
    assert dict(refdoc1.items()) == refdoc1.doc
    # Iterate on document key,value pair
    assert dict([ (k,v) for k,v in refdoc1 ]) == refdoc1.doc
    # Fetch all revisions, using __call__
    revs = refdoc1( revs=True )
    assert revs == refdoc1.all()
    assert revs['_id'] == 'testdoc1', revs['_rev'] == refdoc1._rev
    rev3 = '3-' + revs['_revisions']['ids'][0]
    rev2 = '2-' + revs['_revisions']['ids'][1]
    rev1 = '1-' + revs['_revisions']['ids'][2]
    # Fetch 1st revision, using __call__
    doc1 = refdoc1( rev=rev1 )
    assert doc1._rev == rev1 and doc1['value'] == 1000
    # Fetch extended revision info
    revinfo = refdoc1( revs_info=True )
    assert sorted( map( lambda x : x['rev'], revinfo['_revs_info'] )) == \
           [ rev1, rev2, rev3 ]
    assert sorted( map( lambda x : x['status'], revinfo['_revs_info'] )) == \
           [ 'available', 'available', 'available' ]
    # Dictionary access
    doc1 = Document( db, refdoc1 )
    doc1['helo'] = 'world'
    doc1 = doc1()
    assert doc1._rev.startswith('4-') and doc1['helo'] == 'world'
    # Delete item
    refdoc1()
    refdoc1.delitem( 'helo' )
    assert refdoc1._rev.startswith('5-') and 'helo' not in refdoc1.keys()
    prev_rev = refdoc1._rev
    del refdoc1['value']
    assert refdoc1._rev.startswith('6-') and 'value' not in refdoc1.keys()

    [ c.delete(db.dbname) for db in c ]

def test_delete_document() :
    log.info( "Testing delete document ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    db = c.create( 'testdb' )
    doc = _makedoc( _id='testdoc1', value=1000 )
    doc1 = Document.create( db, doc )
    doc = _makedoc( _id='testdoc2', value=1001 )
    doc2 = Document.create( db, doc )
    assert sorted([ _id for _id in db ]) == ['testdoc1', 'testdoc2']

    Document.delete( db, doc1 )
    assert sorted([ _id for _id in db ]) == ['testdoc2']
    Document.delete( db, doc2._id, rev=doc2._rev )
    assert sorted([ _id for _id in db ]) == []

    [ c.delete(db.dbname) for db in c ]

def test_copy_document() :
    log.info( "Testing copy document ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    db = c.create( 'testdb' )
    doc = _makedoc( _id='testdoc1', value=1000 )
    doc1 = Document.create( db, doc )

    doc2 = doc1.copyto( 'testdoc2' )
    assert doc2._id == 'testdoc2' and doc2._rev.startswith( '1-' )

    doc1['helo'] = 'world'
    doc1().copyto( 'testdoc2', asrev=doc2._rev )
    doc2 = Document( db, 'testdoc2' )
    assert doc2._rev.startswith('2-')

    del doc1['helo']
    doc1()
    Document.copy( db, doc1, 'testdoc2', asrev=doc2._rev )
    doc2 = Document( db, 'testdoc2' )
    assert doc2._rev.startswith('3-')

    [ c.delete(db.dbname) for db in c ]

def test_attachments() :
    log.info( "Testing attachments ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    db = c.create( 'testdb' )
    refdoc1 = _makedoc( _id='testdoc1', value=1000 )
    doc = Document.create( db, refdoc1 )

    # Add attachment
    a1 = doc.addattach(files[0])
    doc()
    data = open( files[0] ).read()
    assert doc._rev.startswith('2-')
    assert a1.content_type == 'text/x-python' and a1.length == len(data)
    assert a1.data() == (data, a1.content_type) and a1.revpos == 2
    a2 = doc.addattach(files[1], content_type='text/xml')
    doc()
    data = open( files[1] ).read()
    assert doc._rev.startswith('3-') and a2.content_type == 'text/xml'
    assert a2.length == len(data) and a2.data() == (data, a2.content_type)
    assert a2.revpos == 3
    attachs = doc.attachs()
    assert (attachs[0] == a1 or attachs[0] == a2) and \
           (attachs[1] == a1 or attachs[1] == a2)

    # Delete attachment
    filename = a2.filename
    doc.delattach( choice([ a2, a2.filename ]) )
    assert doc.attach( filename ) == None

    [ c.delete(db.dbname) for db in c ]

def test_local_doc() :
    log.info( "Testing local doc ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    # Basic
    db = c.create( 'testdb' )
    doc = _makedoc( _id='localdoc', value=1000 )
    ldoc = LocalDocument.create( db, doc )
    assert ldoc._id == '_local/localdoc'
    assert ldoc['value'] == 1000 and ldoc._rev.startswith( '0-1' )

    # Test in batch mode.
    ldoc = _makedoc( _id='localdoc1', value=1001 )
    LocalDocument.create( db, ldoc, batch='ok', fetch=False )
    ldoc = _makedoc( _id='localdoc2', value=1002 )
    LocalDocument.create( db, ldoc, batch='ok' )
    ldoc1 = LocalDocument( db, 'localdoc1' )
    ldoc2 = LocalDocument( db, 'localdoc2' )
    assert ldoc1._id == '_local/localdoc1' and ldoc1._rev.startswith( '0-1' )
    assert ldoc2._id == '_local/localdoc2' and ldoc2._rev.startswith( '0-1' )

    # Copy document
    doc = _makedoc( _id='localdoc3', helo='world' )
    ldoc = LocalDocument.create( db, doc )
    ldoc2 = LocalDocument.copy( db, ldoc._id, 'localdoc2', asrev=ldoc2._rev )
    ldoc2()
    assert ldoc2._rev.startswith('0-2') and ('helo', 'world') in ldoc2.items()

    # Delete document
    LocalDocument.delete( db, ldoc1 )
    LocalDocument.delete( db, ldoc2._id, rev=ldoc2._rev )
    try :
        ldoc1 = LocalDocument( db, 'localdoc1' )
    except couchpy.httperror.ResourceNotFound :
        pass
    else :
        assert False

    try :
        ldoc2 = LocalDocument( db, 'localdoc2' )
    except couchpy.httperror.ResourceNotFound :
        pass
    else :
        assert False

    [ c.delete(db.dbname) for db in c ]


if __name__ == '__main__' :
    test_create_document()
    test_access_document()
    test_attachments()
    test_delete_document()
    test_copy_document()
    test_local_doc()
