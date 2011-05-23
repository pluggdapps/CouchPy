"""A ReSTful client for CouchDB server. Aside from providing a client
interface, :class:`Client` objects provide pythonified way of accessing it, for
instance, client objects behave like a dictionary of databases.

Create a client object,

>>> c = Client()
>>> c()
{ "couchdb" : "Welcome", "version" : "<version>" }

New databases can be created using the `create` method,

>>> db = c.create('dbname_first') # Create
>>> db
<Database 'dbname_first'>
>>> c.create('dbname_second')     # Create another database

Other operations :

>>> 'dbname' in c                       # Membership
False
>>> 'dbname_first' in c                 # Membership
True    
>>> print [ db.dbname for db in c ]     # Iteration
[ 'dbname_first', 'dbname_second' ]
>>> len(c)                              # Length
2
>>> db = c['dbname_second']             # Dict-like accessing the database
<Databse 'dbname_second'>
>>> db.name                             # Database object
>>> del c['dbname_second']              # Delete database
'dbname_second'
>>> [ db.dbname for c.all_dbs() ]       # List of Database objects

Fetch 10 univerally unique IDs

>>> c.uuids( 10 )
[u'a0cf4956301a349a0ecc99370e74331e', u'93e4ec906703b7a00abbfd46b46425fb',
u'401d5e7b6f35315077ebcb157833d11b', u'5e09fa1b652c91fa309084ac8aca4a85',
u'98fcaf62d52f7b8e163154a7a9947449', u'c580763552d24f1658080f77cbf8418f',
u'60d1725cf3963f0d6446878d00a06620', u'414041732f1bbf7a31bb28f0e343437d',
u'edcfee7f35ebcf984e73f5dad3a99dd9', u'b5ce9e5f2b57c39fa37d50ece3cdddb6']

Replicate source database to a target database,

>>> c.replicate( 'blogs', 'blogs-bkp' )

Database operations

>>> c.create( 'blog' )                  # Create database
<Databse 'blog'>
>>> c.database( 'blog' )                # Get an instance of Database
<Databse 'blog'>
>>> c.delete( 'blog' )                  # Delete database
>>> c.has_database( 'blog' )            # Check whether database is present
True
"""

# This file is subject to the terms and conditions defined in
# file 'LICENSE', which is part of this source code package.
#       Copyright (c) 2010 SKR Farms (P) LTD.

import os, sys, logging
from   copy             import deepcopy
from   Cookie           import SimpleCookie

import rest
from   httpc            import HttpSession, OK, ACCEPTED
from   httperror        import *
from   couchpy          import AuthSession
from   couchpy.mixins   import Helpers

# TODO :
#   1. Fix `replicate()` method.
#   2. Deleteing configuration section/key seems to have some complex options.
#      But the API doc is not clear about it.
#   4. Logging is just not working.
#   5. Test cases for addadmin(), deladmin(), admins().

log = logging.getLogger( __name__ )
__VERSION__ = '0.1'
DEFAULT_URL = os.environ.get( 'COUCHDB_URL', 'http://localhost:5984/' )

hdr_acceptjs = { 'Accept' : 'application/json' }
hdr_ctypeform = { 'Content-Type' : 'application/x-www-form-urlencodeddata' }
hdr_ctypejs = { 'Content-Type' : 'application/json' }

def _headsrv( conn, paths=[], hthdrs={} ) :
    """HEAD /"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    s, h, d = conn.head( paths, hthdrs, None )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _getsrv( conn, paths=[], hthdrs={} ) :
    """GET /"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _active_tasks( conn, paths=[], hthdrs={} ) :
    """GET /_active_tasks"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _all_dbs( conn, paths=[], hthdrs={} ) :
    """GET /_all_dbs"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _restart( conn, paths=[], hthdrs={} ) :
    """POST /_restart"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    s, h, d = conn.post( paths, hthdrs, None )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _stats( conn, paths=[], hthdrs={} ) :
    """POST /_stats/"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _uuids( conn, paths=[], hthdrs={}, **query ) :
    """POST /_stats/
    query object `q`,
        count=<num>
    """
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None, _query=query.items() )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _replicate( conn, body, paths=[], hthdrs={} ) :
    """POST /_replicate"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    body = rest.data2json( body )
    s, h, d = conn.post( paths, hthdrs, body )
    if s == ACCEPTED and d['ok'] :
        return s, h, d
    else :
        return (None, None, None)

def _log( conn, paths=[], hthdrs={}, **query ) :
    """GET /_log"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None, _query=query.items() )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _config( conn, paths=[], hthdrs={}, **kwargs ) :
    """
    GET /_config
    GET /_config/<section>
    GET /_config/<section>/<key>
    PUT /_config/<section>/<key>
    DELETE /_config/<section>/<key>
    """
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    if 'value' in kwargs :      # PUT
        body = rest.data2json( kwargs['value'] )
        s, h, d = conn.put( paths, hthdrs, body )
    elif 'delete' in kwargs :
        s, h, d = conn.delete( paths, hthdrs, None )
    else :
        s, h, d = conn.get( paths, hthdrs, None )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _session( conn, paths, login=None, logout=None, hthdrs={}, **kwargs ) :
    """
    GET /_session
    POST /_session
    DELETE /_session
    """
    if logout == True :
        s, h, d = conn.delete( paths, hthdrs, None )
    elif login :
        hthdrs = conn.mixinhdrs( hthdrs, hdr_ctypeform )
        body = 'name=%s&password=%s' % login
        s, h, d = conn.post( paths, hthdrs, body )
    else :
        s, h, d = conn.get( paths, hthdrs, None )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

class Client( object, Helpers ) :

    DEFAULTUSER = 'anonymous'

    def __init__( self, url=DEFAULT_URL, **kwargs) :
        """Initialize a client object, with the base `url` and optional
        key-word arguments.

        ``url``,
            URI to the server, EG, ``http://localhost:5984/``

        ``full_commit``,
            Boolean, turn on the X-Couch-Full-Commit header
        ``hthdrs``,
            Dictionary of HTTP request headers, remembered at the instance
            level.  Aside from these headers, if a method supports `hthdrs` key-word
            argument, it will be used (along with instance-level headers) for a
            single call.
        ``session``,
            :class:`httpc.HttpSession` instance or None for a default session
        ``debug``, 
            for enhanced logging
        ``cookie``,
            `SimpleCookie` cookie object, that can be used to populate headers
        ``defaultuser``,
            Default-user to be returned when user session is not
            authenticated. Used by sessionuser() method.
        """
        self.url = url

        self.full_commit = full_commit = kwargs.get( 'full_commit', None )
        self.htsession = htsession = kwargs.get( 'htsession', HttpSession() )
        self.debug = debug = kwargs.get( 'debug', False )
        self.cookie = cookie = kwargs.get( 'cookie', None )
        self.defaultuser = kwargs.get( 'defaultuser', self.DEFAULTUSER )
        hthdrs = kwargs.get( 'hthdrs', {} )

        if full_commit != None :
            hthdrs.update({ 'X-Couch-Full-Commit' : str(full_commit).lower() })

        self.conn = rest.ReSTful( self.url, htsession, headers=hthdrs )
        self.hthdrs = self.mixinhdrs( hthdrs )
        self.paths = []
        # Loaded the saved cookie so that 
        cookie != None and self.savecookie( self.hthdrs, cookie )
        self._authsession = None

    #---- Pythonification of instance methods. They are supposed to be
    # wrappers around the actual API.

    def __contains__( self, name ) :
        """Return True or False based on whether the server contains
        database specified by ``name``. Refer :func:`Client.has_database`
        """
        return self.has_database(name)

    def __iter__( self ) :
        """Iterate over all databases available in the server.  Each iteration
        yields :class:`couchpy.database.Database` instance corresponding to
        a database in the server.
        Refer :func:`Client.all_dbs`
        """
        return iter( self.all_dbs() )

    def __len__( self ) :
        """Return the count of databases available in the server."""
        return len(self.all_dbs())

    def __nonzero__( self ) :
        """Return whether the server is available."""
        conn, paths = self.conn, self.paths
        s, _, _ = _headsrv( conn, paths, hthdrs=self.hthdrs )
        return True if s == OK else False

    def __repr__( self ) :
        return '<%s %r>' % (type(self).__name__, self.url)

    def __delitem__( self, name ) :
        """Remove the database with the specified ``name``. Refer
        :func:`Client.delete` method
        """
        return self.delete(name)

    def __getitem__( self, name ) :
        """Return a :class:`couchpy.database.Database` instance representing
        database specified by ``name``.
        Refer :func:`Client.database` method
        """
        from   database     import Database
        return self.database(name)

    def __call__( self ) :
        """Check whether CouchDB instance is alive and return the welcome string,
        which will be something like

        >>> c = Client()
        >>> c()
        { "couchdb" : "Welcome", "version" : "<version>" }

        """
        conn, paths = self.conn, self.paths
        s, h, d = _getsrv( conn, paths, hthdrs=self.hthdrs )
        return d


    #---- Database Management System ----

    def version( self ) :
        """Version string from CouchDB server."""
        return self().get( 'version', None )

    def active_tasks( self, hthdrs={} ) :
        """Obtain a list of active tasks. The result is a JSON converted array of
        the currently running tasks, with each task being described with a single
        object.

        >>> c.active_tasks()
        [ {
          "pid"    : "<0.11599.0>",  # Erlang pid
          "status" : "Copied 0 of 18369 changes (0%)",
          "task"   : "recipes",
          "type"   : "Database Compaction"
          },
          ....
        ]

        Admin-Prev, No
        """
        conn, paths = self.conn, (self.paths + [ '_active_tasks' ])
        hthdrs = self.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _active_tasks( conn, paths, hthdrs=hthdrs )
        return d

    def all_dbs( self, hthdrs={} ) :
        """Return a list of all the databases as
        :class:`couchpy.database.Database` objects from the CouchDB server.

        Admin-Prev, No
        """
        from   database     import Database
        conn, paths, debug = self.conn, (self.paths + [ '_all_dbs' ]), self.debug
        hthdrs = self.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _all_dbs( conn, paths, hthdrs=hthdrs )
        return [ Database( self, n, debug=debug) for n in d ] if d else []

    def restart( self, hthdrs={} ) :
        """Restart the CouchDB instance. You must be authenticated as a user
        with administration privileges for this to work. Returns a Boolean,
        indicating success or failure

        Admin-Prev, Yes
        """
        conn, paths = self.conn, (self.paths + [ '_restart' ])
        hthdrs = self.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _restart( conn, paths, hthdrs=hthdrs )
        return d['ok'] if (s==OK) else False

    def stats( self, *paths, **kwargs ) :
        """Return a JSON converted object containting the statistics for the
        CouchDB server. The object is structured with top-level sections
        collating the statistics for a range of entries, with each individual
        statistic being easily identified, and the content of each statistic
        is self-describing.  You can also access individual statistics by
        passing ``statistics-sections`` and ``statistic-ID`` as positional
        arguments.

        >>> c.stats( 'couchdb', 'request_time' )

        Admin-Prev, No
        """
        conn, paths = self.conn, (['_stats'] + list(paths))
        hthdrs = self.mixinhdrs( self.hthdrs, kwargs.get('hthdrs', {}) )
        s, h, d = _stats( conn, paths, hthdrs=hthdrs )
        return d

    def uuids( self, count=None, hthdrs={} ) :
        """Return ``count`` number of uuids, generted by the server. These uuid
        can be used to compose document ids.
        """
        q =  { 'count' : count } if isinstance(count, (int,long)) else {}
        conn, paths = self.conn, (self.paths + ['_uuids'])
        hthdrs = self.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _uuids( conn, paths, hthdrs=hthdrs, **q )
        return d['uuids'] if s == OK else None

    def utils( self ) :
        """To be used with web-interface / browser"""
        log.warn( "_utils/ should be used with a browser to access Futon" )
        return None

    def replicate( self, source, target, hthdrs={}, **options ) :
        """Request, configure, or stop, a replication operation.

        ``source``
            URL of the source database
        ``target``
            URL of the target database

        key-word arguments,

        ``cancel``,
            Cancels the replication
        ``continuous``,
            Boolean to configure the replication to be continuous
        ``create_target``,
            Creates the target database
        ``doc_ids``,
            Array of document IDs to be synchronized
        ``proxy``,
            Address of a proxy server through which replication should occur
        """
        hthdrs = self.mixinhdrs( self.hthdrs, hthdrs )
        conn, paths = self.conn, (self.paths + ['_replicate'])
        # request body
        body = {'source': source, 'target': target}
        body.update(options)
        # request header
        s, h, d = _replicate( conn, body, paths, hthdrs=hthdrs )
        return d

    def log( self, bytes=None, offset=None, hthdrs={} ) :
        """Get CouchDB log, equivalent to accessing the local log file of
        the corresponding CouchDB instance. When you request the log, the
        response is returned as plain (UTF-8) text, with an HTTP Content-type
        header as text/plain. Returns a stream of text bytes.

        ``bytes``,
            Number of bytes to return from tail end of the log.
        ``offset``,
            Offset tail end.

        Admin-Prev, No
        """
        conn, paths = self.conn, (self.paths + ['_log'])
        q = {}
        isinstance(bytes, (int,long)) and q.setdefault('bytes', bytes)
        isinstance(offset, (int,long)) and q.setdefault('offset', offset)
        hthdrs = self.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _log( conn, paths, hthdrs=hthdrs, **q )
        return d.getvalue() if s == OK else None

    def config( self, section=None, key=None, hthdrs={}, **kwargs ) :
        """Configuration of CouchDB server. If ``section`` and ``key`` is not
        specified, returns the entire CouchDB server configuration as a JSON
        converted structure. The structure is organized by different configuration
        sections. If ``section`` parameter is passed, returns the configuration
        structure for a single section specified by ``section``. If
        ``section`` and ``key`` is specified, returns a single configuration
        value from within a specific configuration section.

        To update a particular section/key, provide a keyword argument called
        ``value``. Value will be converted to JSON string and passed on to the
        server.
        
        To delete a particular section/key supply ``delete=True`` keyword
        argument.

        Returns nested dict. of configuration name and value pairs, organized by
        section.

        Admin-Prev, No
        """
        paths = []
        paths.append( section ) if section != None else None
        paths.append( key ) if key != None else None
        value = kwargs.get( 'value', None )
        delete = kwargs.get( 'delete', None )
        conn, paths = self.conn, (['_config'] + paths)
        hthdrs = self.mixinhdrs( self.hthdrs, hthdrs )
        if delete == True :
            s, h, d = _config( conn, paths, hthdrs=hthdrs, delete=delete )
        elif value != None :
            s, h, d = _config( conn, paths, hthdrs=hthdrs, value=value )
        else :
            s, h, d = _config( conn, paths, hthdrs=hthdrs )
        return d

    def addadmin( self, name, password ) :
        """Create a server admin by name ``name`` with ``password``."""
        self.config( section='admins', key=name, value=password )

    def deladmin( self, name ) :
        """Delete server admin user ``name``"""
        self.config( section='admins', key=name, delete=True )

    def admins( self ) :
        """List of admin user"""
        return self.config( section='admins' )

    def login( self, username, password, hthdrs={} ) :
        """Login with ``username`` and ``password``, uses session-cookie for
        authentication, so preserve the following cookie for subsequent
        request.
        """
        if self.cookie : 
            log.warn( 'Client already authenticated (%s)' % self.cookie )
            return (None, None, None)
        conn, paths = self.conn, ['_session']
        hthdrs = self.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _session( conn, paths, login=(username, password), hthdrs=hthdrs )
        self.cookie = sc = SimpleCookie()
        sc.load( h['set-cookie'] )
        self.savecookie( self.hthdrs, sc )
        return s, h, d if s == OK and d['ok'] else (None, None, None)

    def logout( self, hthdrs={} ) :
        """Logout from authenticated DB session"""
        conn, paths = self.conn, ['_session']
        hthdrs = self.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _session( conn, paths, logout=True, hthdrs=hthdrs )

    def authsession( self, hthdrs={} ) :
        """Fetch the authenticated session information for this client. Note
        that browser-session is not handled by the client."""
        if self._authsession == None :
            conn, paths = self.conn, ['_session']
            hthdrs = self.mixinhdrs( self.hthdrs, hthdrs )
            s, h, d = _session( conn, paths, hthdrs=hthdrs )
            self._authsession = AuthSession(d)
        return self._authsession

    def sessionuser( self ) :
        session = self.authsession()
        if session.userCtx :
            return session.userCtx.get( 'name', self.defaultuser )
        else :
            return self.defaultuser
        

    #---- Database,
    #---- the actual ReST-ful API call is made by the Database class

    def create( self, name, hthdrs={} ) :
        """Create a new database with the given ``name``. Return, a
        :class:`couchpy.database.Database` object representing the created
        database.

        Admin-Prev, No
        """
        from   database     import Database
        db = Database.create( self, name, hthdrs=hthdrs )
        return db

    def delete( self, db, hthdrs={} ) :
        """Delete the database db."""
        from   database     import Database
        name = db if isinstance( db, basestring ) else db.dbname
        return Database.delete( self, name, hthdrs=hthdrs )

    def has_database( self, name, hthdrs={} ) :
        """Return whether the server contains a database with the specified
        ``name``. Return, `True` if a database with the ``name`` exists, `False`
        otherwise

        Admin-Prev, No
        """
        from   database     import Database
        db = Database( self, name )
        return db.ispresent()

    def database( self, name ) :
        """Return a :class:`couchpy.database.Database` object representing the
        database with the specified ``name``. Return, a `Database` object
        representing the created database.

        Admin-Prev, No
        """
        from   database     import Database
        db = Database( self, name )
        return db
