#! /usr/bin/env python

import sys
import pprint
import logging
import time
from   random   import choice

# TODO : 
#   1. Test cases to emulate every concievable HTTP STATUS response from the
#   server.
#   2. Test cases for replication

sys.path.insert( 0, '..' )

from client     import Client
from database   import Database
from httperror  import configlog

log = configlog( __name__ )

def test_client() :
    log.info( "Testing client ..." )
    c = Client( url='http://localhost:5984/', debug=True )
    [ c.delete(db.name) for db in c ]

    assert c()['couchdb'] == 'Welcome'
    assert isinstance( c()['version'], basestring )

    [ c.delete(db.name) for db in c ]

def test_active_task() :
    log.info( "Testing active task ..." )
    c = Client( url='http://localhost:5984/', debug=True )
    [ c.delete(db.name) for db in c ]

    ds = c.active_tasks()
    assert isinstance(ds, list)
    for d in ds :
        keys = sorted([ 'pid', 'status', 'task', 'type' ])
        assert sorted(d[0].keys()) == keys

    [ c.delete(db.name) for db in c ]

def test_all_dbs() :
    log.info( "Testing all_dbs ..." )
    c = Client( url='http://localhost:5984/', debug=True )
    [ c.delete(db.name) for db in c ]

    c.create('testdb1')
    c.create('testdb2')
    assert sorted([ x.name for x in c.all_dbs() ]) == ['testdb1', 'testdb2']

    [ c.delete(db.name) for db in c ]

def test_config() :
    log.info( "Testing config ..." )
    c = Client( url='http://localhost:5984/', debug=True )
    [ c.delete(db.name) for db in c ]

    sections = c.config()
    assert len(sections) == 14

    for secname, section in sections.items() :
        assert section == c.config(secname)
        for key, value in section.items() :
            assert value == c.config(secname, key)

    [ c.delete(db.name) for db in c ]

def test_restart() :
    log.info( "Testing restart ..." )
    c = Client( url='http://localhost:5984/', debug=True )
    [ c.delete(db.name) for db in c ]

    c.restart()
    time.sleep(3)

    [ c.delete(db.name) for db in c ]

def test_stats() :
    log.info( "Testing stats ..." )
    c = Client( url='http://localhost:5984/', debug=True )
    [ c.delete(db.name) for db in c ]

    stats = c.stats()
    for statname, stvalue in stats.items() :
        for key, value in stvalue.items() :
            ref = { statname : { key : value } }
            assert ref.keys() == c.stats( statname, key ).keys()

    [ c.delete(db.name) for db in c ]


def test_uuids() :
    log.info( "Testing uuids ..." )
    c = Client( url='http://localhost:5984/', debug=True )
    [ c.delete(db.name) for db in c ]

    uuid = c.uuids()
    assert len(uuid) == 1
    assert int(uuid[0], 16)
    uuids = c.uuids(10)
    assert len(uuids) == 10
    for uuid in uuids :
        assert int(uuid, 16)

    [ c.delete(db.name) for db in c ]


def test_log() :
    log.info( "Testing log ..." )
    c = Client( url='http://localhost:5984/', debug=True )
    [ c.delete(db.name) for db in c ]

    logtext1 = c.log() 
    assert 'GMT' in logtext1
    assert len(logtext1) == 1000
    assert logtext1[:60] not in c.log(bytes=100)
    assert logtext1[900:60] in c.log(bytes=100)
    assert logtext1[:60] in c.log(bytes=100, offset=900)
    assert logtext1[100:60] not in c.log(bytes=100, offset=900)

    [ c.delete(db.name) for db in c ]

def test_database() :
    log.info( 'Testing database methods ...' )
    c = Client( url='http://localhost:5984/', debug=True )
    [ c.delete(db.name) for db in c ]

    c.create('testdb1')
    c.create('testdb2')
    assert 'testdb1' in c
    assert 'testdb2' in c

    dbs = c.all_dbs()
    assert len(c) == 2
    assert sorted([ x.name for x in dbs ]) == ['testdb1', 'testdb2']
    assert c['testdb1'].name == 'testdb1'
    assert c['testdb2'].name == 'testdb2'
    assert c.has_database('testdb1')
    assert c.has_database('testdb2')
    assert isinstance( c.database('testdb1'), Database )
    assert isinstance( c.database('testdb2'), Database )

    del c['testdb1']
    c.delete( choice([ 'testdb2', c['testdb2'] ]) )
    assert 'testdb1' not in c
    assert 'testdb2' not in c
    assert len(c) == 0
    assert sorted([ db.name for db in c ]) == []

if __name__ == '__main__' :
    c = Client( url='http://localhost:5984/', debug=True )
    print 'Testing version %s ' % c.version()
    test_client()
    test_active_task()
    test_all_dbs()
    test_config()
    test_stats()
    test_uuids()
    test_database()
    #test_log()
    #test_restart()
