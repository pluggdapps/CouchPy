#! /usr/bin/env python

import sys, os
import pprint
import logging
from   os.path      import join, abspath, basename

sys.path.insert( 0, '..' )

from   couchpy.client    import Client
from   couchpy.database  import Database
from   couchpy.doc       import Document
from   couchpy.httperror import *

log = configlog( __name__ )

def _makedoc( **kwargs ) :
    return kwargs 

def test_create_document() :
    log.info( "Testing create document ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    files = map( lambda f : abspath(f), os.listdir('.'))
    db = c.create( 'testdb' )
    doc = _makedoc( _id='testdoc', value=1000 )
    f = basename(files[0])
    doc = Document.create( db, doc, attachfiles=f )
    assert doc._id == 'testdoc'
    assert doc['value'] == 1000
    a = doc.attach( f )
    assert a.filename == f
    assert a.data() == (open( files[0] ).read(), a.content_type)

    [ c.delete(db.dbname) for db in c ]

def test_delete_document() :
    log.info( "Testing delete document ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    [ c.delete(db.dbname) for db in c ]

def test_copy_document() :
    log.info( "Testing copy document ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    [ c.delete(db.dbname) for db in c ]

def test_access_document() :
    log.info( "Testing access document ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    [ c.delete(db.dbname) for db in c ]

def test_refresh_document() :
    log.info( "Testing refresh document ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    [ c.delete(db.dbname) for db in c ]

def test_update_document() :
    log.info( "Testing update document ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    [ c.delete(db.dbname) for db in c ]

def test_attachments() :
    log.info( "Testing attachments ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    [ c.delete(db.dbname) for db in c ]

if __name__ == '__main__' :
    test_create_document()
    test_delete_document()
    test_copy_document()
    test_access_document()
    test_refresh_document()
    test_update_document()
    test_attachments()
