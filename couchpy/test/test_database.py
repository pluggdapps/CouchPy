#! /usr/bin/env python

import sys, pprint, logging

from   couchpy.client       import Client
from   couchpy.database     import Database
from   couchpy.doc          import Document
from   couchpy.httperror    import *

# TODO :
#   1. Test cases for _changes with,
#           feed, filter, heartbeat, timeout

log = logging.getLogger( __name__ )

def _makedoc( **kwargs ) :
    return kwargs 

def test_create_database() :
    log.info( "Testing create database ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    db = c.create( 'testdb' )

    # Database information
    info = db()
    assert sorted(info.keys()) == sorted([
            u'update_seq', u'disk_size', u'purge_seq', u'doc_count',
            u'compact_running', u'db_name', u'doc_del_count',
            u'instance_start_time', u'committed_update_seq',
            u'disk_format_version',
           ])

    [ c.delete(db.dbname) for db in c ]
    
def test_access_database() :
    log.info( "Testing access database ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    db = c.create( 'testdb' )
    doc = _makedoc( _id='testdoc1', value=1000 )
    doc1 = db.createdoc( docs=doc )
    doc.update( _id='testdoc2', helo='world' )
    doc2 = db.createdoc( docs=doc )

    # Iterate on database documents
    assert map(None, db) == [ 'testdoc1', 'testdoc2' ]

    # Count of document
    assert len(db) == 2

    # Get documents
    doc = db['testdoc1'] 
    assert doc._id == 'testdoc1' and doc._rev.startswith( '1-' ) and \
           doc['value'] == 1000

    # Delete documents
    doc = _makedoc( _id='testdoc3', value=1000 )
    db.createdoc( docs=doc )
    del db['testdoc3']
    assert 'testdoc3' not in db and 'testdoc1' in db

    # Create documents

    # Fetch documents

    # Delete documents

    # Copy documents

    [ c.delete(db.dbname) for db in c ]

def test_designdocs() :
    log.info( "Testing designdocs ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    [ c.delete(db.dbname) for db in c ]
    
def test_changes_database() :
    log.info( "Testing changes ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    [ c.delete(db.dbname) for db in c ]
    
def test_compact_database() :
    log.info( "Testing compact database ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    [ c.delete(db.dbname) for db in c ]
    
def test_viewcleanup_database() :
    log.info( "Testing viewcleanup database ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    [ c.delete(db.dbname) for db in c ]
    
def test_ensurefullcommit_database() :
    log.info( "Testing ensurefullcommit ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    [ c.delete(db.dbname) for db in c ]
    
def test_bulkdocs_database() :
    log.info( "Testing bulkdocs ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    [ c.delete(db.dbname) for db in c ]
    
def test_tempview_database() :
    log.info( "Testing tempview ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    [ c.delete(db.dbname) for db in c ]
    
def test_purge_database() :
    log.info( "Testing tempview ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    [ c.delete(db.dbname) for db in c ]
    
def test_revslimit() :
    log.info( "Testing revision limit to preserve ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    [ c.delete(db.dbname) for db in c ]

def test_attributes() :
    log.info( "Testing attributes ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.dbname) for db in c ]

    [ c.delete(db.dbname) for db in c ]

    
if __name__ == '__main__' :
    test_create_database()
    test_access_database()
    #test_designdocs()
    #test_changes_database()
    #test_compact_database()
    #test_viewcleanup_database()
    #test_ensurefullcommit_database()
    #test_bulkdocs_database()
    #test_tempview_database()
    #test_purge_database()
    #test_revslimit()
    #test_attributes()
