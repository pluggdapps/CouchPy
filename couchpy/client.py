"""
Client library in python for CouchDB's ReSTful API.
"""

# This file is subject to the terms and conditions defined in
# file 'LICENSE', which is part of this source code package.
#       Copyright (c) 2010 SKR Farms (P) LTD.

import os, re, sys, logging
from   copy                  import deepcopy

import rest
import database
from   database     import Database
from   httpc        import HttpSession, ResourceNotFound, OK, CREATED
from   httperror    import *

# TODO :
#   1. Fix `replicate()` method.
#   2. Deleteing configuration section/key seems to have some complex options.
#      But the API doc is not clear about it.
#   3. Move configlog from httperror to __init__.py
#   4. Logging is just not working.

__VERSION__ = '0.1'
DEFAULT_URL = os.environ.get( 'COUCHDB_URL', 'http://localhost:5984/' )
log = configlog( __name__ )

class Client( object ) :
    """
    A ReSTful client for CouchDB server. This class behaves like a dictionary
    of databases. For example, to get a list of database names on the server,
    you can simply iterate over the server object. To make all client-request
    from the object to include a standard set of http headers, pass in the
    optional headers keyword argument.

    Initialize the server object, with the base url and optional
    key-word arguments.

    url, URI to the server, EG, ``http://localhost:5984/``

    Optional key-word arguments,

    full_commit,
        Boolean, turn on the X-Couch-Full-Commit header
    headers,
        an http.Session instance or None for a default session
    session,
        an http.Session instance or None for a default session
    debug, 
        for enhanced logging

    >>> c = Client()

    New databases can be created using the `create` method:

    >>> db = c.create('dbname') # Create
    >>> db
    <Database 'dbname'>

    Other operations :

    >>> 'dbname' in c           # Membership
    >>> [ db for db in c ]      # Iteration
    >>> len(c)                  # Length
    >>> del c['dbname']         # Delete
    >>> db = c['dbname']        # Dict-access
    >>> db.name                 # Database object
    'dbname'
    """

    def __init__( self, url=DEFAULT_URL, **kwargs) :
        self.url = url
        self._makeclient( **kwargs )

    def _makeclient( self, **kwargs ) :
        full_commit = kwargs.get( 'full_commit', None )
        hthdrs = kwargs.get( 'hthdrs', {} )
        htsession = kwargs.get( 'htsession', HttpSession() )
        debug = kwargs.get( 'debug', False )

        hthdrs = {
            'X-Couch-Full-Commit' :  str(full_commit).lower()
        } if full_commit != None else {}

        h_ = deepcopy( hthdrs )
        h_.update({ 'Accept' : 'application/json' })
        self.rest = rest.ReSTful( self.url, htsession, headers=hthdrs )
        self.restjs = rest.ReSTful( self.url, htsession, headers=h_ )

        self.hthdrs, self.htsession, self.debug = hthdrs, htsession, debug
        return

    #---- Pythonification, all the methods are just wrappers around the API
    #---- methods

    def __contains__( self, name ) :
        """
        Return whether the server contains a database with the specified
        name. Refer `has_database`
        """
        return self.has_database(name)

    def __iter__( self ) :
        """
        Iterate over the names of all databases. Each iteration yields
        db.Database instance
        """
        return iter( self.all_dbs() )

    def __len__( self ) :
        """
        Return the number of databases.
        """
        return len(self.all_dbs())

    def __nonzero__( self ) :
        """
        Return whether the server is available.
        """
        try :
            s, _, _ = self.rest.head( '', {}, None )
            return True if s == OK else False
        except :
            log.error( sys.exc_info() )
            if self.debug : raise
            return False

    def __repr__( self ) :
        return '<%s %r>' % (type(self).__name__, self.url)

    def __delitem__( self, name ) :
        """
        Remove the database with the specified name.
        """
        return self.delete(name)

    def __getitem__( self, name ) :
        """
        Return a `Database` object representing the database with the
        specified name.

        name, the name of the database
        """
        return self.database(name)

    def __call__( self ) :
        """
        Check whether the database is alive and return the welcome string,
        Return,
            { "couchdb" : "Welcome", "version" : "<version>" }
        """
        try :
            s, h, d = self.restjs.get('', {}, None)
            return d if (s==OK) and (d['couchdb']=='Welcome') else None
        except ResourceNotFound, error :
            log.error( 'ResourceNotFound : %s', error )
            if self.debug : raise
            return None


    #---- Database Management System ----

    def version( self ) :
        """
        The version string of the CouchDB server.
        """
        d = self()
        return d.get('version', None) if d else None

    def active_tasks( self ) :
        """
        List of running tasks, including the task type, name, status and
        process ID.

        Return,
              [ {
                "pid"    : "<0.11599.0>",  # Erlang pid
                "status" : "Copied 0 of 18369 changes (0%)",
                "task"   : "recipes",
                "type"   : "Database Compaction"
                },
                ....
              ]
        Admin-Prev : No
        """
        try :
            s, h, d = self.restjs.get( '_active_tasks', {}, None )
            return d if s == OK else None
        except :
            log.error( sys.exc_info() )
            if self.debug : raise
            return None

    def all_dbs( self ) :
        """
        List all database.

        Return, List of `Database` objects.
        Admin-Prev : No
        """
        all_dbs = []
        try :
            s, h, d = self.restjs.get('_all_dbs', {}, None)
            return [ Database(
                        self.url, dbn, htsession=self.htsession, client=self,
                        debug=self.debug
                     ) for dbn in d ] if s == OK else None
        except :
            log.error( sys.exc_info() )
            if self.debug : raise
            return None

    def config( self, *paths, **kwargs ) :
        """
        Configuration of CouchDB server. Specify the ``secion`` and ``key``
        as positional arguments. Optional _query keyword argument is a list of
        key,value tuples. To update a particular section/key, provide
        a keyword argument called value. To delete a particular section/key
        supply delete=True keyword argument.

        Return,
           Returns nested dict. of configuration name and value pairs,
           organized by section.
        Admin-Prev :: No
        """
        paths = list(paths)
        paths.insert(0, '_config')
        value = kwargs.get( 'value', None )
        delete = kwargs.get( 'delete', None )
        try :
            if delete == True :
                s, h, d = self.restjs.delete( paths, {}, None )
            elif value != None :
                h = { 'Content-Type' : 'application/json' }
                s, h, d = self.restjs.put( paths, h, value )
            else :
                s, h, d = self.restjs.get( paths, {}, None )
            return d if s == OK else None
        except :
            log.error( sys.exc_info() )
            if self.debug : raise
            return None

    def restart( self ) :
        """
        Restarts the CouchDB instance. You must be authenticated as a user
        with administration privileges for this to work.

        Return, Boolean, indicating success or failure
        Admin-Prev, Yes
        """
        try :
            h = { 'Content-Type' : 'application/json' }
            s, h, d = self.restjs.post( '_restart', h, None )
            return d['ok'] if (s==OK) else False
        except :
            log.error( sys.exc_info() )
            if self.debug : raise
            return False

    def stats( self, *paths ) :
        """Database statistics, from resource pointed by url/path

        Return,
            Returns a nested dict. of name and value pairs, organized
            by section.
        Admin-Prev :: No
        """
        paths = list(paths)
        paths.insert(0, '_stats')
        try :
            s, h, d = self.restjs.get( paths, {}, None )
            return d if s == OK else None
        except :
            log.error( sys.exc_info() )
            if self.debug : raise
            return None

    def uuids( self, count=None ) :
        """Retrieve a count number of uuids

        Return, a list of uuids
        """
        try :
            q = []
            isinstance(count, (int,long)) and q.append(( 'count', count ))
            s, h, d = self.restjs.get( '_uuids', {}, None, _query=q )
            return d['uuids'] if s == OK else None
        except :
            log.error( sys.exc_info() )
            if self.debug : raise
            return None

    def utils( self ) :
        """To be used with web-interface / browser"""
        log.warn( "_utils/ should be used with a browser to access Futon" )
        return None

    def replicate( self, source, target, **options ) :
        """
        Replicate changes from the source database to the target database.

        source, URL of the source database
        target, URL of the target database
        options, optional replication args, e.g. continuous=True
        """
        body = {'source': source, 'target': target}
        body.update(options)
        data = StringIO()
        json.dump(body, data)
        data.seek(0)
        body = data.read()
        try :
            h = { 'Content-Type' : 'application/json' }
            s, h, d = self.restjs.post( '_replicate', h, body )
            return d
        except :
            log.error( sys.exc_info() )
            if self.debug : raise
            return None

    def log( self, bytes=None, offset=None ) :
        """
        Return access log from CouchDB server, as a stream of UTF-8 encoded
        plain text.

        bytes, Number of bytes to return from tail end of the log.
        offset, Offset tail end.
        return, Stream of text bytes.
        Admin-Prev :: No
        """
        try :
            q = []
            isinstance(bytes, (int,long)) and q.append(('bytes', bytes))
            isinstance(offset, (int,long)) and q.append(('offset', offset))
            s, h, d = self.restjs.get( '_log', {}, None, _query=q )
            return d.read() if s == OK else None
        except :
            log.error( sys.exc_info() )
            if self.debug : raise
            return None

    #---- Database,
    #---- the actual ReST-ful API call is made by the Database class

    def create( self, name ) :
        """
        Create a new database with the given name.

        Return, a `Database` object representing the created database
        Admin-Prev, No
        """
        db = Database( self.url, name, htsession=self.htsession,
                       client=self, debug=self.debug, create=True )
        return db

    def delete( self, db ) :
        """
        Delete the database db.
        """
        if isinstance( db, Database ) :
            db = Database( self.url, db, htsession=self.htsession,
                           client=self, debug=self.debug )
        return db.delete()

    def has_database( self, name ) :
        """
        Return whether the server contains a database with the specified
        name.

        Return, `True` if a database with the name exists, `False` otherwise
        Admin-Prev : No
        """
        if isinstance( name, Database ) :
            db = name
        else :
            db = Database( self.url, name, htsession=self.htsession,
                           client=self, debug=self.debug )
        return db.ispresent()

    def database( self, name ) :
        """Return a `Database` object representing the database with the
        specified name.

        Return, a `Database` object representing the created database
        Admin-Prev : No
        """
        db = Database( self.url, name, htsession=self.htsession, client=self,
                       debug=self.debug )
        return db if db.ispresent() else None
