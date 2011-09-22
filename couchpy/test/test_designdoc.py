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

from   client       import Client
from   database     import Database
from   httperror    import *

log = logging.getLogger( __name__ )

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
