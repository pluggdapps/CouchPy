#!/usr/bin/env python

# This file is subject to the terms and conditions defined in
# file 'LICENSE', which is part of this source code package.
#       Copyright (c) 2011 SKR Farms (P) LTD.

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
    c = Client( url='http://pratap:pratap@localhost:5984/' )
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
