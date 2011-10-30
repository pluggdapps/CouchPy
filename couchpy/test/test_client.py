#!/usr/bin/env python

# This file is subject to the terms and conditions defined in
# file 'LICENSE', which is part of this source code package.
#       Copyright (c) 2011 SKR Farms (P) LTD.

# -*- coding: utf-8 -*-

import sys, logging, time, pprint
from   random               import choice
from   couchpy.client       import Client
from   couchpy.database     import Database

log = logging.getLogger( __name__ )

def test_info( url ) :
    print "Testing client ..."
    c = Client( url=url )
    assert c.url, url

    # __nonzero__
    assert bool(c)

    assert c()['version'] == c.version()

def test_basics( url ) :
    print "Testing client in python way ..."
    c = Client( url=url )
    ca = Client( url=url )
    ca.login( 'pratap', 'pratap' )

    ca.delete('testdb1') if 'testdb1' in ca else None
    ca.delete('testdb2') if 'testdb2' in ca else None
    ca.delete('testdb3') if 'testdb3' in ca else None

    print "Testing put() ..."
    ca.put('testdb1')
    ca.put('testdb2')

    print "Testing __contains__ operation ..."
    assert 'testdb1' in c
    assert 'testdb2' in c
    
    print "Testing __iter__ operation ..."
    databases = sorted([ db.dbname for db in c ])
    assert 'testdb1' in databases
    assert 'testdb2' in databases

    print "Testing __len__ operation and put(), delete() methods ..."
    a = len(c)
    assert a >= 2
    ca.put('testdb3')
    assert len(c) == a+1

    print "Testing __getitem__, __delitem__ operation and put() method ..."
    assert isinstance( c['testdb3'], Database )
    assert c['testdb3'].dbname == 'testdb3'
    del ca['testdb3']

    print "Testing .databases properties ..."
    databases = sorted([ db.dbname for db in c.databases ])
    assert 'testdb1' in databases
    assert 'testdb2' in databases

    print "Testing active_tasks() method ..."
    ds = ca.active_tasks()
    assert isinstance(ds, list)
    for d in ds :
        keys = sorted([ 'pid', 'status', 'task', 'type' ])
        assert sorted(d[0].keys()) == keys

    print "Testing all_dbs() method ..."
    assert sorted(c.all_dbs()) == sorted( map(lambda x : x.dbname, c))

    print "Testing log() method ..."
    logtext1 = ca.log()
    assert 'GMT' in logtext1
    assert len(logtext1) == 1000
    assert logtext1[:60] not in ca.log(bytes=100)
    assert logtext1[900:60] in ca.log(bytes=100)
    #assert logtext1[:60] in ca.log(bytes=100, offset=900)
    #assert logtext1[100:60] not in ca.log(bytes=100, offset=900)

    print "Testing stats() method ..."
    stats = c.stats()
    for statname, stvalue in stats.items() :
        for key, value in stvalue.items() :
            ref = { statname : { key : value } }
            assert ref.keys() == c.stats( statname, key ).keys()

    print "Testing uuids() method ..."
    uuid = c.uuids()
    assert len(uuid) == 1
    assert int(uuid[0], 16)
    uuids = c.uuids(10)
    assert len(uuids) == 10
    for uuid in uuids :
        assert int(uuid, 16)


    print "Testing authentication and administration ..."
    c.login( 'pratap', 'pratap' )
    assert c.authsession()['userCtx']['name']  ==   'pratap'
    assert c.authsession()['userCtx']['roles'] == [ '_admin' ]
    assert c.sessionuser == 'pratap'
    if True :                   # config()
        sections = c.config()
        assert len(sections) > 10
        for secname, section in sections.items() :
            assert section == c.config(secname)
            for key, value in section.items() :
                assert value == c.config(secname, key)
        c.config( 'uuids', 'algorithm', value='utc_random' )
        assert c.config( 'uuids', 'algorithm', value='utc_random' ) == 'utc_random'
        c.config( 'uuids', 'algorithm', delete=True )
    if True :                   # administration
        assert c.admins().keys() == [ 'pratap' ] 
        c.addadmin( 'joe', 'joe' )
        assert sorted( c.admins().keys() ) == sorted([ 'pratap', 'joe' ])
        c.deladmin( 'joe' )
    c.logout()
    try :
        c.config()
        assert False
    except :
        pass

    print "Testing Database ..."
    db = c.Database( 'testdb1' )
    assert isinstance( db, Database )
    assert db.dbname == 'testdb1'

    print "Testing delete() method ..."
    ca.delete( 'testdb1' )
    ca.delete( 'testdb2' )

if __name__ == '__main__' :
    url = 'http://localhost:5984/'
    c = Client( url=url )
    print 'CouchDB version %s' % c.version()
    test_info( url )
    test_basics( url )
    print
