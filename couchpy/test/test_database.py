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

    c = Client( url=url )
    ca = Client( url=url )
    ca.login( 'pratap', 'pratap' )

    [ db.delete() for db in ca.databases ]

    print "Testing instantiation ..."
    db1 = ca.put( 'testdb1' )
    db2 = ca.put( 'testdb2' )
    c.Database( 'testdb1' )
    c.Database( 'testdb2' )
    print "Testing `opendbs` in client  ..."
    assert sorted( c.opendbs.keys() ) == [ 'testdb1', 'testdb2' ]
    assert ca.Database( 'testdb1' ) == ca.Database( 'testdb1' )
    assert ca.Database( 'testdb1' ) != ca.Database( 'testdb2' )
    print "Testing `singleton_docs` in database instance  ..."
    assert db1.singleton_docs == { 'active' : {}, 'cache' : {} }
    db3 = ca.put( 'testdb3' )
    assert len(c) == 3

    dba = ca.put( 'testdb' )
    db  = c.Database( 'testdb' )

    print "Testing .info property and __call__ operation ..."
    info = db()
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
    joe1doc.delete()
    assert 'joe1' not in db
    joe1doc = db.Document( 'joe1', rev=joe1doc._rev ).fetch()
    assert joe1doc._deleted

    print "Testing ispresent() method ..."
    assert db.ispresent()

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

def test_changes( url ):
    c = Client( url=url )
    ca = Client( url=url )
    ca.login( 'pratap', 'pratap' )
    [ db.delete() for db in ca.databases ]

    dba = ca.put( 'testdb' )
    db = c.Database( 'testdb' )
    # Create doc
    db.Document( sampledoc ).post()
    db.Document( sampledoc1 ).post()
    # Update doc
    doc = db.Document( sampledoc['_id'] ).fetch()
    doc.field1 = 10
    doc.put()

    print "Testing changes() method ..."
    d = db.changes(feed='normal', since=1)
    assert d['results'][0]['id'] == 'joe1'
    assert d['results'][1]['id'] == 'joe'
    assert d['results'][0]['changes'][0]['rev'].startswith('1-')
    assert d['results'][1]['changes'][0]['rev'].startswith('2-')
    d = db.changes(feed='normal', since=1, include_docs='true')
    assert all([ 'doc' in x for x in d['results'] ])

def continuous_changes( url ):
    c = Client( url=url )
    db = c.Database( 'testdb' )
    print 'Reading changes ...'
    def fn( line ) :
        print line
    db.changes( feed='continuous', callback=fn, timeout=1000 )

if __name__ == '__main__' :
    url = 'http://localhost:5984/'
    #logging.basicConfig( level=logging.INFO )
    c = Client( url=url )
    print 'CouchDB version %s' % c.version()
    test_basics( url )
    test_changes( url )
    #continuous_changes( url )
