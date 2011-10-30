# This file is subject to the terms and conditions defined in
# file 'LICENSE', which is part of this source code package.
#       Copyright (c) 2011 SKR Farms (P) LTD.

# -*- coding: utf-8 -*-

""":class:`Client` implements required client interface for a CouchDB
server. Aside from this, :class:`Client` instance provide pythonified way of
accessing the database systems, for example, a :class:`Client` instance behave
like a dictionary of databases. A :class:`Client` constructor typically takes
httpurl (for server) as its argument. If url is not provided, ENVIRONMENT
variable ``COUCHDB_URL`` will be used if available, if that is also not
available then the default url 'http://localhost:5984/' will be assumed.

A collection of examples using :class:`Client` objects.

Create a client object,

>>> couch = Client()

Check for server availability,

>>> bool( couch )
True

Get server information,

>>> couch()
{ "couchdb" : "Welcome", "version" : "<version>" }

Get active tasks in DB server, (required admin privileges)

>>> couch.active_tasks()
[]

Number of databases present in the server,

>>> len(couch)
3

Get a list of all available database names,

>>> couch.all_dbs()
[ '_users', 'sessions', 'bootdb' ]

Iterate over all databases in the server, returns a list of 
:class:`couchpy.database.Database` objects,

>>> [ db for db in couch ]
[ <Database u'sessions'>, <Database u'bootdb'>, <Database u'_users'> ]

Check whether a database is present,

>>> couch.has_database( '_users' )
True
>>> '_users' in couch               # Python way
True
>>> 'contacts' in couch
False

Get :class:`couchpy.database.Database` object as a dictionary key from client,

>>> usersdb = couch['_users']
>>> couch['sessions']
<Database 'sessions'>

Get server statistics,

>>> couch.stats()
{ ... }     # A dictionary of server statistics

Get a list of 2 univerally unique IDs, the count argument is optional.

>>> c.uuids( 2 )
[ u'a0cf4956301a349a0ecc99370e74331e', u'93e4ec906703b7a00abbfd46b46425fb' ]


Operations that require admin privileges :

Restart couchdb server instance,

>>> couch.restart()

Get server log (required admin privileges)

>>> logs = couch.log().splitlines()
>>> couch.log( offset=100, bytes=10 )

Server configuration

>>> couch.config()      # Get the current configuration dictionary, section wise
>>> couch.config( section='uuids' )
{u'algorithm': u'utc_random'}
>>> couch.config( section='uuids', key='algorithm' )
u'utc_random'
>>> couch.config( section='uuids', key='algorithm', value='random') # Update
>>> couch.config( section='uuids', key='algorithm' )                
u'random'
>>> couch.config( section='uuids', key='algorithm', delete=True )   # Delete

>>> couch.addadmin( 'sokochi', 'joe123' ) # Add a server admin (username,passwd)
>>> couch.deladmin( 'sokochi' )           # Delete a server admin
>>> couch.admins()                        # List of server admins
{ 'pratap' : u'-hashed-cf8f18ed9a17e6d6....' }

Server authetication and session,

>>> couch.login( username, password )
>>> couch.authsession() # Authenticated user's session information
>>> couch.logout()      # Authenticated user will be identified via cookie.

Database operations :

>>> c.put('blog')                       # Create database
<Databse 'blog'>
>>> c.Database('blog')                  # Get the Database instance
<Databse 'blog'>
>>> c.delete( 'blog' )                  # Delete database
>>> c.has_database( 'blog' )            # Check whether database is present
True

Miscellaneous operation,

>>> couch.version()
u'1.0.1'
>>> couch.ispresent()
True
"""

import os, logging
from   Cookie           import SimpleCookie

import rest
from   httpc            import HttpSession, OK, ACCEPTED
from   httperror        import *
from   couchpy          import hdr_acceptjs, hdr_ctypejs, hdr_ctypeform, \
                               hdr_accepttxtplain, hdr_acceptany, \
                               __version__, defconfig

# TODO :
#   1. Deleteing configuration section/key seems to have some complex options.
#      But the API doc is not clear about it.
#   2. Logging is just not working.

log = logging.getLogger( __name__ )
__version__ = __version__
DEFAULT_URL = os.environ.get( 'COUCHDB_URL', 'http://localhost:5984/' )

def _headsrv( conn, paths=[], hthdrs={} ) :
    """HEAD /"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    s, h, d = conn.head( paths, hthdrs, None )
    if s != OK :
        s = h = d = None
        log.warn( 'HEAD / request failed' )
    return s, h, d

def _getsrv( conn, paths=[], hthdrs={} ) :
    """GET /"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None )
    if s != OK :
        s = h = d = None
        log.warn( 'GET / request failed' )
    return s, h, d

def _active_tasks( conn, paths=[], hthdrs={} ) :
    """GET /_active_tasks"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None )
    if s != OK :
        s = h = d = None
        log.error( 'GET /_active_tasks request failed' )
    return s, h, d

def _all_dbs( conn, paths=[], hthdrs={} ) :
    """GET /_all_dbs"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None )
    if s != OK :
        s = h = d = None
        log.error( 'GET /_all_dbs request failed' )
    return s, h, d

def _restart( conn, paths=[], hthdrs={} ) :
    """POST /_restart"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    s, h, d = conn.post( paths, hthdrs, None )
    if s != OK :
        s = h = d = None
        log.error( 'POST /_restart request failed' )
    return s, h, d

def _stats( conn, paths=[], hthdrs={} ) :
    """POST /_stats/"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None )
    if s != OK :
        s = h = d = None
        log.error( 'POST /_stats request failed' )
    return s, h, d

def _uuids( conn, paths=[], hthdrs={}, **query ) :
    """POST /_uuids
    query object `q`,
        count=<num>
    """
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None, _query=query.items() )
    if s != OK :
        s = h = d = None
        log.error( 'POST /_uuids request failed' )
    return s, h, d

def _replicate( conn, body, paths=[], hthdrs={} ) :
    """POST /_replicate"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    body = rest.data2json( body )
    s, h, d = conn.post( paths, hthdrs, body )
    if (s != ACCEPTED) or (not d['ok']) :
        s = h = d = None
        log.error( 'POST /_replicate request failed' )
    return s, h, d

def _log( conn, paths=[], hthdrs={}, **query ) :
    """GET /_log
    query parameters
        offset=<num>
        bytes=<num>
    parameters are json encoded
    """
    hthdrs = conn.mixinhdrs( hthdrs, hdr_accepttxtplain )
    #if isinstance( query.get('offset', None), (int,long) ) :
    #    query['offset'] = '%r' % str(query['offset'])
    #if isinstance( query.get('bytes', None), (int,long) ) :
    #    query['bytes']  = '%r' % str(query['bytes'])
    s, h, d = conn.get( paths, hthdrs, None, _query=query.items() )
    if s != OK :
        s = h = d = None
        log.error( 'POST /_log request failed' )
    return s, h, d

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
        method = 'PUT'
        s, h, d = conn.put( paths, hthdrs, body )
    elif 'delete' in kwargs :
        method = 'DELETE'
        s, h, d = conn.delete( paths, hthdrs, None )
    else :
        method = 'GET'
        s, h, d = conn.get( paths, hthdrs, None )
    if s != OK :
        s = h = d = None
        log.error( '%s request to (%s) failed' % (method, paths) )
    return s, h, d

def _session( conn, paths, login=None, logout=None, hthdrs={}, **kwargs ) :
    """
    GET /_session
    POST /_session
    DELETE /_session
    """
    if logout == True :
        method = 'DELETE'
        s, h, d = conn.delete( paths, hthdrs, None )
    elif login :
        method = 'POST'
        hthdrs = conn.mixinhdrs( hthdrs, hdr_ctypeform )
        body = 'name=%s&password=%s' % login
        s, h, d = conn.post( paths, hthdrs, body )
    else :
        method = 'GET'
        hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
        s, h, d = conn.get( paths, hthdrs, None )
    if s != OK :
        s = h = d = None
        log.error( '%s request to /_session failed' % (method, paths) )
    return s, h, d


#---- Client class

class Client( object ) :
    """Client interface for CouchDB server.

    ``url``,
        Server http url.
    ``pyconfig``,
        Configuration parameters.
    ``hthdrs``,
        Dictionary of HTTP request headers, remembered at the instance
        level.  Aside from these headers, if a method supports `hthdrs` key-word
        argument, it will be used (overriding ``hthdrs``) for a
        single call.
    ``cookie``
        cookie value for authenticated sessions. Value can be ``basestring`` or
        cookie.SimpleCookie object.
    """

    def __init__( self, url=None, pyconfig=None, hthdrs=None, cookie=None ) :
        self.hthdrs = hthdrs or {}
        self.cookie = cookie
        self.pyconfig = dict( defconfig.items() )
        self.pyconfig.update( pyconfig or {} )

        self.url = url or self.pyconfig['realm']
        self.conn = rest.ReSTful(self.url, HttpSession(), headers=self.hthdrs)
        cookie and self.conn.savecookie( self.hthdrs, cookie )

        self.paths = []
        self.available = None       # assume that server is not available
        # Load the saved cookie to preserve the authentication
        self._authsession, self.opendbs = None, {}


    #---- Pythonification of instance methods. They are supposed to be
    #---- wrappers around the actual API.

    def __contains__( self, name ) :
        """Return ``True`` if database ``name`` is present, else ``False``. Refer
        :func:`Client.has_database`
        """
        return self.has_database(name)

    def __iter__( self ) :
        """Iterate over all databases available in the server. Each iteration
        yields :class:`couchpy.database.Database` instance corresponding to
        a database in the server. Refer :func:`Client.all_dbs`
        """
        return self.DatabaseIterator()

    def __len__( self ) :
        """Return the count of database available in the server."""
        return len(self.all_dbs())

    def __nonzero__( self ) :
        """If returns ``True``, then the server is available. This is essentially
        a one time check to know whether the database instance pointed by
        `realm` is available. Subsequent checks will simply return the
        remembered status.  To make a fresh request for server availability use,
        :func:`Client.ispresent` method."""
        if self.available == None :
            self.available = self.ispresent()
        return self.available

    def __repr__( self ) :
        return '<%s %r>' % (type(self).__name__, self.url)

    def __delitem__( self, name ) :
        """Remove database ``name`` from server. Refer :func:`Client.delete`
        method.
        """
        return self.delete(name)

    def __getitem__( self, name ) :
        """Return a :class:`couchpy.database.Database` instance for database,
        ``name``. Refer :func:`Client.database` method
        """
        return self.Database(name)

    def __call__( self ) :
        """Check whether CouchDB instance is alive and return the welcome
        string, which will be something like

        >>> c = Client()
        >>> c()
        { "couchdb" : "Welcome", "version" : "<version>" }

        To just check for server availability, use :func:`Client.ispresent`
        method.
        """
        conn, paths = self.conn, self.paths
        s, h, d = _getsrv( conn, paths, hthdrs=self.hthdrs )
        return d


    #---- API methods for Database Management System

    def version( self ):
        """Version string from CouchDB server."""
        return self().get( 'version', None )

    def ispresent( self ):
        """Do a fresh check for server availability. This will emit a `HEAD`
        request to the server. For efficiency reasons you can just use the
        boolean operator on the :class:`Client` instance like,

        >>> bool( couch )
        """
        conn, paths = self.conn, self.paths
        s, _, _ = _headsrv( conn, paths, hthdrs=self.hthdrs )
        self.available = s == OK
        return self.available

    def active_tasks( self, hthdrs={} ) :
        """Obtain a list of active tasks. The result is a JSON converted array of
        the currently running tasks, with each task being described with a single
        object.

        >>> couch.active_tasks()
        [ {
          "pid"    : "<0.11599.0>",  # Erlang pid
          "status" : "Copied 0 of 18369 changes (0%)",
          "task"   : "recipes",
          "type"   : "Database Compaction"
          },
          ....
        ]

        Admin-Prev, Yes
        """
        conn, paths = self.conn, (self.paths + [ '_active_tasks' ])
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _active_tasks( conn, paths, hthdrs=hthdrs )
        return d

    def all_dbs( self, hthdrs={} ) :
        """Return a list of all the databases as
        :class:`couchpy.database.Database` objects from the CouchDB server.

        Admin-Prev, No
        """
        from   database     import Database
        conn, paths = self.conn, (self.paths + [ '_all_dbs' ])
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _all_dbs( conn, paths, hthdrs=hthdrs )
        return d if d else []

    def log( self, bytes=None, offset=None, hthdrs={} ) :
        """Get CouchDB log, equivalent to accessing the local log file of
        the corresponding CouchDB instance. When you request the log, the
        response is returned as plain (UTF-8) text, with an HTTP Content-type
        header as text/plain. Returns a stream of text bytes.

        ``bytes``,
            Bytes to be returned.
        ``offset``,
            Offset in bytes where the log tail should be started.

        Admin-Prev, Yes
        """
        conn, paths = self.conn, (self.paths + ['_log'])
        q = {}
        isinstance(bytes, (int,long)) and q.setdefault('bytes', bytes)
        isinstance(offset, (int,long)) and q.setdefault('offset', offset)
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _log( conn, paths, hthdrs=hthdrs, **q )
        return d.getvalue() if s == OK else None

    #def log( self, bytes=None, offset=None, hthdrs={} ) :
    #    """Returns a log iterator that can be used to iterate over the list of
    #    log messages.
    #    """
    #    return Log( self, bytes=bytes, offset=offset )

    def restart( self, hthdrs={} ) :
        """Restart the CouchDB instance. You must be authenticated as a user
        with administrative privileges for this to work. Returns a Boolean,
        indicating success or failure

        Admin-Prev, Yes
        """
        conn, paths = self.conn, (self.paths + [ '_restart' ])
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _restart( conn, paths, hthdrs=hthdrs )
        return d['ok'] if (s==OK) else False

    def stats( self, *paths, **kwargs ) :
        """Return a JSON converted object containting the statistics for this
        CouchDB server. The object is structured with top-level sections
        collating the statistics for a range of entries, with each individual
        statistic being easily identified, and the content of each statistic
        is self-describing.  You can also access individual statistics by
        passing ``statistics-section`` and ``statistic-id`` as positional
        arguments, similiar to the following example

        >>> c.stats( 'couchdb', 'request_time' )
        {'couchdb': {'request_time': {'current': 1265.6880000000001,
                                      'description': 'length of a request inside CouchDB without MochiWeb',
                                      'max': 235.0,
                                      'mean': 14.717000000000001,
                                      'min': 3.0,
                                      'stddev': 29.754999999999999,
                                      'sum': 1265.6880000000001} } }

        Admin-Prev, No
        """
        conn, paths = self.conn, (['_stats'] + list(paths))
        hthdrs = conn.mixinhdrs( self.hthdrs, kwargs.get('hthdrs', {}) )
        s, h, d = _stats( conn, paths, hthdrs=hthdrs )
        return d

    def uuids( self, count=None, hthdrs={} ) :
        """Return ``count`` number of uuids, generated by the server. These uuid
        can be used to compose document ids.
        """
        q =  { 'count' : count } if isinstance(count, (int,long)) else {}
        conn, paths = self.conn, (self.paths + ['_uuids'])
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _uuids( conn, paths, hthdrs=hthdrs, **q )
        return d['uuids'] if s == OK else None

    #---- Server configuration API

    def config( self, section=None, key=None, hthdrs={}, **kwargs ) :
        """Configuration of CouchDB server. If ``section`` and ``key`` is not
        specified, returns the entire CouchDB server configuration as a JSON
        converted structure. The structure is organized by different configuration
        sections.

        If ``section`` parameter is passed, returns the configuration
        structure for a single section specified by ``section``.

        If ``section`` and ``key`` is specified, returns a single configuration
        value from within a specific configuration section.

        To update a particular section/key, provide a keyword argument called
        ``value``. Value will be converted to JSON string and passed on to the
        server.
        
        To delete a particular section/key supply ``delete=True`` keyword
        argument.

        Returns nested dict. of configuration name and value pairs, organized by
        section.

        Admin-Prev, Yes
        """
        paths = []
        paths.append( section ) if section != None else None
        paths.append( key ) if key != None else None
        value = kwargs.get( 'value', None )
        delete = kwargs.get( 'delete', None )
        conn, paths = self.conn, (['_config'] + paths)
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        if delete == True :
            s, h, d = _config( conn, paths, hthdrs=hthdrs, delete=delete )
        elif value != None :
            s, h, d = _config( conn, paths, hthdrs=hthdrs, value=value )
        else :
            s, h, d = _config( conn, paths, hthdrs=hthdrs )
        return d

    #---- Server authentication-administration API methods

    def addadmin( self, name, password ) :
        """Create a server admin by name ``name`` with ``password``.
        
        Admin-Prev, Yes
        """
        self.config( section='admins', key=name, value=password )

    def deladmin( self, name ) :
        """Delete server admin user ``name``

        Admin-Prev, Yes
        """
        self.config( section='admins', key=name, delete=True )

    def admins( self ) :
        """List of admin user

        Admin-Prev, Yes
        """
        return self.config( section='admins' )

    def login( self, username, password, hthdrs={} ) :
        """Login with ``username`` and ``password``, uses session-cookie for
        authentication, so preserve the following cookie for subsequent
        request.

        Returns a tuple of (status, header, payload) from HTTP response,
        header contains the cookie information if the login was successful,
        this cookie can be preserved to make authenticated request to the DB
        server.
        Authentication cookie is remembered for all subsequent request via
        this client instance.
        """
        if self.cookie : 
            log.warn( 'Client already authenticated (%s)' % self.cookie )
            return (None, None, None)
        conn, paths = self.conn, ['_session']
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _session( conn, paths, login=(username, password), hthdrs=hthdrs )
        self.cookie = sc = SimpleCookie()
        sc.load( h['set-cookie'] )
        self.conn.savecookie( self.hthdrs, sc )  # Save the cookie in `self.hthdrs`
        return s, h, d if s == OK and d['ok'] else (None, None, None)

    def logout( self, hthdrs={} ) :
        """Logout from authenticated DB session. The authentication cookie is
        no longer valid.
        """
        conn, paths = self.conn, ['_session']
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        self._authsession = None
        s, h, d = _session( conn, paths, logout=True, hthdrs=hthdrs )
        self.hthdrs.pop( 'Cookie', None )

    def authsession( self, hthdrs={} ) :
        """Fetch the authenticated session information for this client. At any
        point only one user can remain authenticated for a single client
        instance. Note that browser-session is not handled by the client.
        """
        from  couchpy   import AuthSession
        if self._authsession == None :
            conn, paths = self.conn, ['_session']
            hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
            s, h, d = _session( conn, paths, hthdrs=hthdrs )
            self._authsession = AuthSession(d)
        return self._authsession

    def _sessionuser( self ) :
        session = self.authsession()
        c = session.userCtx
        return c.get( 'name', None ) if c else None


    #---- Database APIs via client object,
    #---- the actual ReST-ful API call is made by the Database class

    def put( self, name, hthdrs={} ) :
        """Create a new database with the given ``name``. Return, a
        :class:`couchpy.database.Database` object representing the created
        database.

        Admin-Prev, Yes
        """
        return self.Database(name, hthdrs=hthdrs).put()

    def delete( self, db, hthdrs={} ) :
        """Delete the database ``db``, which can be passed as a instance of
        :class:`couchpy.database.Database` class or as database-name.
        
        Admin-Prev, Yes
        """
        ( self.Database(db) if isinstance( db, basestring ) else db 
        ).delete( hthdrs=hthdrs )
        return None

    def has_database( self, name, hthdrs={} ) :
        """Return whether the server contains a database with the specified
        ``name``. Return, ``True`` if a database with the ``name`` exists,
        ``False`` otherwise

        Admin-Prev, No
        """
        return self.Database(name).ispresent()

    def Database( self, name, *args, **kwargs ):
        """Convenience method to access the Database class under the context
        of the client instance.
        Return a :class:`couchpy.database.Database` object representing the
        database with the specified ``name``.

        Admin-Prev, No
        """
        from   database     import Database
        return Database( self, name, *args, **kwargs )

    def DatabaseIterator( self, regexp=None ):
        from   database     import DatabaseIterator
        return DatabaseIterator( self, values=self.all_dbs(), regexp=regexp )


    def commit( self ):
        """ -- TBD -- This is part multi-document access design, which is still
        evolving.

        Call to this method will bulk commit all the active documents
        (dirtied) under every open database for this client.

        """
        [ db.commit() for dbname, db in self.opendbs.items() ]

    #---- Place holder API methods

    def utils( self ) :
        """To be used with web-interface / browser"""
        log.warn( "_utils/ should be used with a browser to access Futon" )
        return None

    # TODO : This is a evolving feature. A lot needs to be done :)
    def replicate( self, source, target, hthdrs={}, **options ) :
        """ -- TBD --

        Request, configure, or stop, a replication operation.

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
        conn, paths = self.conn, (self.paths + ['_replicate'])
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        # request body
        body = {'source': source, 'target': target}
        body.update(options)
        # request header
        s, h, d = _replicate( conn, body, paths, hthdrs=hthdrs )
        return d

    #---- Properties

    databases = property( lambda self : [ self.Database(n) for n in self.all_dbs() ])
    sessionuser = property( _sessionuser )


class Log( object ):
    """Iterate over couchdb server's log messages"""

    def Message( object ):
        def __init__( self, message ):
            self.message = message

        def __str__( self ):
            return '%s' % self.message

        def __repr__( self ):
            return '%r' % self.message

    def __init__( self, client, bytecount=None, offset=None ):
        self.bytecount = bytecount
        self.offset = 0
        self.rawlog = client.rawlog( bytes=bytecount, offset=offset )

    def __iter__( self ):
        pass

    def next( self ):
        pass

    def _parselog( self, text ):
        logs = text.split('\n\n')
