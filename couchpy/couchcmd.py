#!/usr/bin/env python

# This file is subject to the terms and conditions defined in
# file 'LICENSE', which is part of this source code package.
#       Copyright (c) 2011 SKR Farms (P) LTD.

import optparse, sys, pprint, logging
from   couchpy.client import Client

def empty() :
    pass

def option_parse() :
    """Parse the options and check whether the semantics are correct."""
    parser  = optparse.OptionParser(usage="usage: %prog [options] url" )
    parser.add_option( '-g', dest='loginfo', action="store_true",
                       default=False,
                       help='Information logging' )
    parser.add_option( '-a', dest='alive', action="store_true",
                       default=False,
                       help='Check whether server is alive' )
    parser.add_option( '-l', dest='listdbs', action="store_true",
                       default=False,
                       help='List available database names' )
    parser.add_option( '-t', dest='activetasks', action="store_true",
                       default=False,
                       help='List active-tasks' )
    parser.add_option( '-s', dest='stats', action="store_true",
                       default=False,
                       help='Server statistics' )
    parser.add_option( '-c', dest='config', action="store_true",
                       default=False,
                       help='List of configuration parameters' )
    parser.add_option( '-n', dest='confsec', type="string",
                       default='',
                       help='Configuration paramters for section' )
    parser.add_option( '-m', dest='admins', action="store_true",
                       default=False,
                       help='List of admin users' )
    parser.add_option( '-p', dest='putdb', type="string",
                       default='',
                       help='Put (create) database name in server' )
    parser.add_option( '-d', dest='deldb', type="string",
                       default='',
                       help='Delete database name from server' )
    parser.add_option( '-u', dest='authsess', action="store_true",
                       default=False,
                       help='Authenticated session information.' )
    options, args   = parser.parse_args()
    return parser, options, args

if __name__ == '__main__' :
    parser, options, args = option_parse()
    url = (args and args[0]) or 'http://localhost:5984'
    client = Client( url )
    if bool( client ) == False :
        print "Server (%s) is not available" % url
        sys.exit(1)
    if options.loginfo :
        logging.basicConfig( level=logging.INFO )

    if options.alive :
        print client()
    elif options.activetasks :
        pprint.pprint( client.active_tasks() )
    elif options.stats :
        pprint.pprint( client.stats() )
    elif options.config :
        if options.confsec :
            pprint.pprint( client.config( section=options.confsec ) )
        else :
            pprint.pprint( client.config() )
    elif options.admins :
        print client.admins()
    elif options.listdbs :
        dbs = client.all_dbs()
        print "(%s) %s" % (len(dbs), dbs)
    elif options.deldb :
        print "Deleting database %r ..."  % options.deldb
        client.delete( options.deldb )
    elif options.putdb :
        print "Putting database %r ..."  % options.putdb
        client.put( options.putdb )
    else :
        print "Hello : %s\n" % client()
        print "Databases available from %s : " % url
        print "  (%s) %s" % ( len(client), ', '.join(client.all_dbs()) )
        print "\n"
        parser.print_help()
