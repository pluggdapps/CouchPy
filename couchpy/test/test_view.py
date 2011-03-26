#! /usr/bin/env python

import sys
import pprint
import logging

sys.path.insert( 0, '..' )

from   couchpy.client       import Client
from   couchpy.database     import Database
from   couchpy.query        import Query
from   httperror            import *

log = configlog( __name__ )

def test_query() :
    log.info( "Testing query ..." )

    q = Query(params={ 'startkey' : '10', 'limit' : 2 })
    assert q.query() in [ '?limit=2&startkey=10', '?startkey=10&limit=2' ]
    q = Query( startkey='10', limit=2 )
    assert q.query() in [ '?limit=2&startkey=10', '?startkey=10&limit=2' ]

    q['startkey'] = '20'
    q.update({ 'endkey' : '40' })
    assert sorted(q.query()[1:].split('&')) == [
                'endkey=40', 'limit=2', 'startkey=20'
           ]
    assert q['startkey'] == '20'

    q = q( endkey='50' )
    assert sorted(q.query()[1:].split('&')) == [
                'endkey=50', 'limit=2', 'startkey=20'
           ]

if __name__ == '__main__' :
    test_query()
