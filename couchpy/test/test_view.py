#! /usr/bin/env python

import sys, pprint, logging

from   couchpy.client       import Client
from   couchpy.database     import Database
from   couchpy.query        import Query
from   httperror            import *

log = logging.getLogger( __name__ )

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
