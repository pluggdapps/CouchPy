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

import sys, pprint, logging

from   couchpy.client       import Client
from   couchpy.database     import Database
from   couchpy.doc          import Query
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
