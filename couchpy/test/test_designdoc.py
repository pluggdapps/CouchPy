#! /usr/bin/env python

import sys
import pprint
import logging

sys.path.insert( 0, '..' )

from   client     import Client
from   database   import Database
from   httperror   import *

log = configlog( __name__ )

def test_get_database() :
    log.info( "Testing Get database ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.name) for db in c ]

    c.create('testdb1')
    db = c['testdb1']
    d = db()
    stats = [ "compact_running", "committed_update_seq", "disk_format_version",
              "disk_size", "doc_count", "doc_del_count", "db_name",
              "instance_start_time", "purge_seq", "update_seq", ]
    assert sorted(d.keys()) == sorted(stats)

    db = Database( 'http://localhost:5984/', 'invalid' )
    d = db() 
    assert d == {}

    [ c.delete(db.name) for db in c ]
    
def test_put_database() :
    log.info( "Testing Put database ..." )

    c = Client( url='http://localhost:5984/' )
    [ c.delete(db.name) for db in c ]

    db = c.create('testdb1')
    assert isinstance( db, Database )

    assert c.create('testdb1') == None
    try :
        c.create('Testdb1') == None
    except InvalidDBname:
        pass
    else :
        assert False

    [ c.delete(db.name) for db in c ]


if __name__ == '__main__' :
    test_get_database()
    test_put_database()
