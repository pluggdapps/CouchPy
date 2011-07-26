#! /usr/bin/env python

import sys, pprint, logging, time, pprint
from   copy                 import deepcopy
from   random               import choice

from   couchpy.client       import Client
from   couchpy.database     import Database
from   couchpy.doc          import Document
from   couchpy.httperror    import *

log = logging.getLogger( __name__ )

sampledoc = {
    '_id'    : 'joe',
    'name'   : 'joe',
    'dob'    : '17/08/70',
    'father' : 'basha',
    'mother' : 'Lui',
}
sampledoc1 = deepcopy( sampledoc )
sampledoc1.update( _id='joe1', name='joe1' )

secobj = {
  "admins": { "names": [ 'joe' ], "roles": [] },
  "readers":{ "names": [ 'joe' ], "roles": []}
}

def test_basics( url ) :
    print "Testing create database ..."

    c = Client( url=url, debug=True )
    ca = Client( url=url, debug=True )
    ca.login( 'pratap', 'pratap' )

    [ db.delete() for db in ca.databases ]

    print "Testing instantiation ..."
    db1 = ca.put( 'testdb1' )
    db2 = ca.put( 'testdb2' )
    assert db1.singleton_docs == Database._singleton_docs[db1.dbname]
    db3 = ca.put( 'testdb3' )
    assert len(c) == 3
    assert db1.dbname in db3._singleton_docs
    assert db2.dbname in db3._singleton_docs
    assert db3.dbname in db3._singleton_docs

    dba = ca.put( 'testdb' )
    db  = c.Database( 'testdb' )

    print "Testing .info property and __call__ operation ..."
    info = db()
    assert info == db.info
    assert db.info is info
    assert sorted(info.keys()) == sorted([
            u'update_seq', u'disk_size', u'purge_seq', u'doc_count',
            u'compact_running', u'db_name', u'doc_del_count',
            u'instance_start_time', u'committed_update_seq',
            u'disk_format_version',
           ])

    print "Testing Document instantiation ..."
    doc = db.Document( sampledoc ).post()
    db.Document( sampledoc1 ).post()

    print "Testing __iter__, __getitem__ operation ..."
    assert sorted( map( None, db )) == [ db['joe'], db['joe1'] ]

    print "Testing __nonzero__ operation ..."
    assert bool( db )

    print "Testing __getitem__, __delitem__, __contains__ operations ..."
    joedoc = db['joe']
    joedoc.fetch()
    assert isinstance( joedoc, Document )
    assert joedoc._id == 'joe'
    assert joedoc.name == 'joe'
    assert 'joe1' in db
    assert 'paris' not in db
    joe1doc = db['joe1']
    del dba['joe1']
    assert 'joe1' not in db
    joe1doc = db.Document( 'joe1', rev=joe1doc._rev ).fetch()
    assert joe1doc._deleted

    print "Testing changes() method ..."
    test_changes( db )

    print "Testing blukdocs() bulkdelete() methods ..."
    docs = {}
    for i in range(10) :
        doc = deepcopy(sampledoc)
        doc['_id'] = doc['_id'] + str(i)
        docs[ doc['_id'] ] = doc
    docs1 = deepcopy( docs )
    # Create non-atomic
    result = db.bulkdocs( docs=docs.values() )
    [ docs[ d['id'] ].update( _rev=d['rev'] ) for d in result ]
    for r in result   : assert r['rev'].startswith('1-')
    for docid in docs : assert docid in db
    # Delete non-atomic
    result = db.bulkdelete( docs=docs.values() )
    for r in result   : assert r['rev'].startswith('2-')
    for docid in docs : assert docid not in db
    # Create atomic
    result = db1.bulkdocs( docs=docs1.values(), atomic=True )
    [ docs1[ d['id'] ].update( _rev=d['rev'] ) for d in result ]
    for r in result   : assert r['rev'].startswith('1-')
    for docid in docs1: assert docid in db1
    # Delete atomic
    result = db1.bulkdelete( docs=docs1.values(), atomic=True )
    for r in result   : assert r['rev'].startswith('2-')
    for docid in docs1: assert docid not in db1

    print "Testing all_docs() method ..."
    docs1 = {}
    for i in range(10) :
        doc = deepcopy(sampledoc)
        doc['_id'] = doc['_id'] + str(i)
        docs1[ doc['_id'] ] = doc
    db.bulkdocs( docs=docs1.values() )
    ids = sorted( map( lambda d : d['id'], db.all_docs()['rows'] ))
    assert ids == sorted( docs1.keys() + ['joe'] )
    ids = sorted( map( lambda d : d['id'], db.all_docs( keys=docs1.keys() )['rows'] ))
    assert ids == sorted( docs1.keys() )

    print "Testing compact() method ..."
    dba.compact()
    assert '_compact' in ca.log( bytes=200 )

    print "Testing viewcleanup() method ..."
    dba.viewcleanup()
    assert '_view_cleanup' in ca.log( bytes=200 )

    print "Testing ensurefullcommit() method ..."
    dba.ensurefullcommit()
    assert '_ensure_full_commi' in ca.log( bytes=200 )

    print "Testing purge() method ..."
    time.sleep(1)
    dba.purge( docs.values() )
    doc = docs.values()[0]
    try    : dba.Document( doc['_id'], rev=doc['_rev'] ).fetch()
    except : pass
    else   : assert False

    print "Testing security() method ..."
    dba.security( secobj )
    assert dba.security() == secobj

    print "Testing revslimit() method ..."
    dba.revslimit( 111 )
    assert dba.revslimit() == 111

    print "Testing delete() method ..."
    dba.delete()
    assert bool( dba ) == False

def test_changes( db ):
    mode = choice([ 'normal', 'continuous', 'longpoll' ])
    query = {}
    query.update( feed=mode ) if choice([ True, False ]) else None
    query.update( heartbeat=100 ) if choice([ True, False ]) else None
    query.update( include_docs='true' ) if choice([ True, False ]) else None
    query.update( limit=10 ) if choice([ True, False ]) else None
    query.update( since=2 ) if choice([ True, False ]) else None
    query.update( timeout=100 ) if choice([ True, False ]) else None

    d = db.changes()
    assert 'results' in d
    assert len(d['results']) == 2


if __name__ == '__main__' :
    url = 'http://localhost:5984/'
    c = Client( url=url, debug=True )
    print 'CouchDB version %s' % c.version()
    test_basics( url )
