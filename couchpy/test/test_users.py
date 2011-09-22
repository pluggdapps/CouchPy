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

import pprint, logging, time
from   random   import choice

from   couchpy.client       import Client
from   couchpy.database     import Database

log = logging.getLogger( __name__ )

def populate_user( c ) :
    log.info( "Testing client ..." )
    

def test_client() :
    log.info( "Testing client ..." )
    c = Client( url='http://pratap:pratap@localhost:5984/', debug=True )
    [ c.delete(db.dbname) for db in c ]

    [ c.delete(db.dbname) for db in c ]

def option_parse() :
    """Parse the options and check whether the semantics are correct."""
    parser = optparse.OptionParser( usage="usage: %prog [options]" )
    #parser.add_option( '-p', dest='logfile', type='string', \
    #                    default='', help='Log file name' ) 
    parser.add_option(
        '-p', dest='populate', action='store_true', default=False,
        help='Populate _users database'
    ) 
    parser.add_option(
        '-t', dest='unititest', action='store_true', default=False,
        help='Do unit-testing'
    ) 
    options, args = parser.parse_args()
    return parser, options, args


if __name__ == '__main__' :
    c = Client( url='http://localhost:5984/', debug=True )
    parser, options, args = option_parse()
    if options.populate :
        populate(c)
    if option.unittest :
        test_client()
