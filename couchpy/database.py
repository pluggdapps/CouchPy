"""Database definition for accessing a CouchDB database. An instance of
:class:`Database` class corresponds to a single database in CouchDB server,
the object can be used to interface with it. Aside from that,
:class:`Database` objects provide pythonified way of accessing it.

Create a database,

>>> couch = Client()
>>> db = couch.put('contacts')  # Create

Example list of operations that can be done with :class:`Database` instance :

Get database information,
>>> db.info
{ ... }

Create a database,
>>> db = couch.Database('blog').put()

Delete the database,
>>> db.delete()

Track changes done to database,
>>> db.changes()

Compact database,
>>> db.compact()

View cleanup,
>>> db.viewcleanup()

Ensure full commit,
>>> db.ensurefullcommit()

Bulk document inserts,
>>> docs = [ {'fruit' : 'orange'}, {'fruit' : 'apple'} ]
>>> db.bulkdocs( docs )
>>> db.bulkdocs( docs, atomic=True )

Get / Set security object,
>>> db.security({ "admins" : { "names" : ["joe"] } })
>>> db.security()
{u'admins': {u'names': [u'pratap']}}

Get / Set revs_limit
>>> db.revslimit( 122 )
>>> db.revslimit()
122

Iterate over all the documents in the database. The following example emits a
list of document ids.
>>> docs = [ doc for doc in db ]
[u'011b9da9723dea64c645554bcf0261619efd68a1',
 u'05ee18cc4b79572594aea1d71253d71538609597',
 ...
]

Access the document like database dictionary,
>>> u'011b9da9723dea64c645554bcf0261619efd68a1' in db
True
>>> db[u'011b9da9723dea64c645554bcf0261619efd68a1']
<Document u'011b9da9723dea64c645554bcf0261619efd68a1':u'1-16565128019675206d77f6836039af0e'>

Remove document from database
>>> del db[u'011b9da9723dea64c645554bcf0261619efd68a1']

Number documents stored in the database,
>>> len(db)

Check for database availability,
>>> bool( db )
"""

import sys, re, logging

import rest
from   couchpy      import hdr_acceptjs, hdr_ctypejs, BaseIterator
from   httperror    import *
from   httpc        import HttpSession, ResourceNotFound, OK, CREATED, ACCEPTED
from   doc          import Document, LocalDocument, DesignDocument, Query

log = logging.getLogger( __name__ )

def _getdb( conn, paths=[], hthdrs={} ) :
    """GET /<db>"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    s, h, d = conn.get( paths, hthdrs, None )
    if s == OK :
        return s, h, d
    else :
        log.error( 'GET request to /%s failed' % '/'.join(paths) )
        return (None, None, None)

def _putdb( conn, paths=[], hthdrs={} ) :
    """PUT /<db>"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    s, h, d = conn.put( paths, hthdrs, None )
    if s == CREATED and d['ok'] == True :
        return s, h, d
    else :
        log.error( 'PUT request to /%s failed' % '/'.join(paths) )
        return (None, None, None)

def _deletedb( conn, paths=[], hthdrs={} ) :
    """DELETE /<db>"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    s, h, d = conn.delete( paths, hthdrs, None )
    if s == OK and d['ok'] == True :
        return s, h, d
    else :
        log.error( 'DELETE request to /%s failed' % '/'.join(paths) )
        return (None, None, None)

def _changes( conn, paths=[], hthdrs={}, **query ) :
    """GET /<db>/_changes
    query,
        feed=normal | continuous | longpoll
        filter=<design-doc>/<func-name>     heartbeat=<milliseconds>
        include_docs=<bool>                 limit=<number>
        since=<seq-num>                     timeout=<millisecond>
    """
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None, _query=query.items() )
    if s == OK :
        return s, h, d
    else :
        log.error( 'GET request to /%s failed' % '/'.join(paths) )
        return (None, None, None)

def _compact( conn, paths=[], hthdrs={} ) :
    """POST /<db>/_compact              To compact database
       POST /<db>/_compact>/<designdoc> To compact view
    """
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    s, h, d = conn.post( paths, hthdrs, None )
    if s == ACCEPTED and d["ok"] == True :
        return s, h, d
    else :
        log.error( 'POST request to /%s failed' % '/'.join(paths) )
        return (None, None, None)

def _view_cleanup( conn, paths=[], hthdrs={} ) :
    """POST /<db>/_view_cleanup"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    s, h, d = conn.post( paths, hthdrs, None )
    if s == ACCEPTED and d["ok"] == True :
        return s, h, d
    else :
        log.error( 'POST request to /%s failed' % '/'.join(paths) )
        return (None, None, None)

def _ensure_full_commit( conn, paths=[], hthdrs={} ) :
    """POST /<db>/_ensure_full_commit"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    s, h, d = conn.post( paths, hthdrs, None )
    if s == CREATED  and d["ok"] == True :
        return s, h, d
    else :
        log.error( 'POST request to /%s failed' % '/'.join(paths) )
        return (None, None, None)

def _bulk_docs( conn, docs, atomic=False, paths=[], hthdrs={} ) :
    """POST /<db>/_bulk_docs"""
    docs = {
        'all_or_nothing' : atomic,
        'docs' : docs,
    }
    body = rest.data2json( docs )
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    s, h, d = conn.post( paths, hthdrs, body )
    if s == CREATED :
        return s, h, d
    else :
        log.error( 'POST request to /%s failed' % '/'.join(paths) )
        return (None, None, None)

def _temp_view( conn, designdoc, paths=[], hthdrs={}, **query ) :
    """POST /<db>/_temp_view
    query,
        Same query parameters as that of design-doc views
    """
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    body = rest.data2json( designdoc )
    s, h, d = conn.post( paths, hthdrs, body, _query=query.items() )
    if s == OK :
        return s, h, d
    else :
        log.error( 'POST request to /%s failed' % '/'.join(paths) )
        return (None, None, None)

def _purge( conn, body, paths=[], hthdrs={} ) :
    """POST /<db>/_purge"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    body = rest.data2json( body )
    s, h, d = conn.post( paths, hthdrs, body )
    if s == OK :
        return s, h, d
    else :
        log.error( 'POST request to /%s failed' % '/'.join(paths) )
        return (None, None, None)

def _all_docs( conn, keys=None, paths=[], hthdrs={}, q={} ) :
    """
    GET  /<db>/_all_docs,     if keys is None
    POST /<db>/_all_docs,    if keys is a list of document keys to select
    query object `q` for GET
        descending=<bool>   endkey=<key>        endkey_docid=<id>
        group=<bool>        group_level=<num>   include_docs=<bool>
        key=<key>           limit=<num>         inclusive_end=<bool>
        reduce=<bool>       skip=<num>          stale='ok'
        startkey=<key>      startkey_docid=<id> update_seq=<bool>
    Note that `q` object should provide .items() method with will return a
    list of key,value query parameters.
    """
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    if keys == None :
        method = 'GET'
        s, h, d = conn.get( paths, hthdrs, None, _query=q.items() )
    else :
        method ='POST'
        body = rest.data2json({ 'keys' : keys })
        s, h, d = conn.post( paths, hthdrs, body, _query=q.items() )
    if s == OK :
        return s, h, d
    else :
        log.error( '%s request to /%s failed' % (method, '/'.join(paths)) )
        return (None, None, None)

def _missing_revs( conn, revs=[], paths=[], hthdrs={} ) :
    """TODO : To be implemented"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    body = rest.data2json( revs )
    s, h, d = conn.post( paths, hthdrs, body )
    if s == OK :
        return s, h, d
    else :
        log.error( 'POST request to /%s failed' % '/'.join(paths) )
        return (None, None, None)

def _revs_diff( conn, revs=[], paths=[], hthdrs={} ) :
    """TODO : To be implemented"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    body = rest.data2json( revs )
    s, h, d = conn.post( paths, hthdrs, body )
    if s == OK :
        return s, h, d
    else :
        log.error( 'POST request to /%s failed' % '/'.join(paths) )
        return (None, None, None)

def _security( conn, paths=[], security=None, hthdrs={} ) :
    """
    GET /<db>/_security
    PUT /<db>/_security
    """
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    if security == None :
        method = 'GET'
        s, h, d = conn.get( paths, hthdrs, None )
    else :
        method = 'PUT'
        body = rest.data2json( security )
        s, h, d = conn.put( paths, hthdrs, body )
    if s == OK :
        return s, h, d
    else :
        log.error( '%s request to /%s failed' % (method, '/'.join(paths)) )
        return (None, None, None)

def _revs_limit( conn, paths=[], limit=None, hthdrs={} ) :
    """
    GET /<db>/_revs_limit       if limit is None
    PUT /<db>/_revs_limit       if limit is an integer value
    """
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    body = '%s' % limit if limit != None else None
    if limit == None :
        method = 'GET'
        s, h, d = conn.get( paths, hthdrs, body )
    else :
        method = 'PUT'
        s, h, d = conn.put( paths, hthdrs, body )
    if s == OK :
        return s, h, d
    else :
        log.error( '%s request to /%s failed' % (method, '/'.join(paths)) )
        return (None, None, None)



class Database( object ) :
    """Instantiate the database object corresponding to ``dbname`` in
    CouchdDB server provided by ``client``. Client's connection will be
    used for all CouchDB access.

    Optional arguments :

    ``debug``, 
        for enhanced logging, if not specified, client's `debug` attribute
        will be used.

    ``hthdrs``,
        Dictionary of HTTP request headers. The header fields and values
        will be remembered for every request made via this ``database``
        object. Aside from these headers, if a method supports `hthdrs`
        key-word argument, it will be used for a single request.
    """

    _singleton_docs = {}   # { <dbname>  : { 'active' : {...}, 'cache' : {...} }

    def __init__( self, client, dbname, hthdrs={}, **kwargs ) :
        self.client, self.conn = client, client.conn
        self.dbname = Database.validate_dbname( dbname )
        self.debug = kwargs.pop( 'debug', client.debug )
        self.hthdrs = self.conn.mixinhdrs( self.client.hthdrs, hthdrs )

        self.paths = client.paths + [ dbname ]
        self._info = {}
        self.hthdrs = self.conn.mixinhdrs( self.client.hthdrs, hthdrs )

        # Every time a Document object is instantiated it will be moved to the
        # `active` list. Once commit() method is called on the database
        # instance, all dirty documents will be commited to the server and
        # moved to `cached` list.
        Database._singleton_docs.setdefault(
            self.dbname, { 'active' : {}, 'cache' : {} } 
        )

    
    #---- Pythonification of instance methods. They are supposed to be
    #---- wrappers around the actual API.

    def __call__( self ) :
        """Return information about this database, refer to ``GET /db`` API
        from CouchDB reference manual to know the structure of information.
        Every time the object is called, database information will be fetched
        from the server. For efficiency reasons, access database information
        via `info` attribute, which will fetch the information only once and
        cache them for subsequent use.

        Admin-Prev, No
        """
        s, h, d = _getdb( self.conn, self.paths, hthdrs=self.hthdrs )
        self._info = d if d != None else {}
        return self._info

    def __iter__( self ) :
        """Iterate over all documents in this database. For every
        iteration, :class:`couchpy.doc.Document` value will be yielded.

        Admin-prev, No
        """
        d = self.all_docs()
        return iter( map( lambda x : self.Document( x['id'] ), d['rows'] ))

    def __getitem__( self, key ) :
        """Return a :class:`couchpy.doc.Document` instance, for the document
        specified by ``key`` which is document's `_id`.

        Admin-prev, No
        """
        return Document( self, key, fetch=True )

    def __len__( self ) :
        """Return number of documents in the database.

        Admin-prev, No
        """
        d = self.all_docs()
        return d['total_rows']

    def __nonzero__(self):
        """Return a boolean, on database availability in the server. Python
        way of bool-check for :func:`Database.ispresent`"""
        return self.ispresent()

    def __delitem__(self, docid) :
        """Remove the document specified by ``docid`` from database"""
        doc =  Document( self, docid )
        headers = doc.head()
        etag = headers['Etag'][1:-1]    # Strip the leading and trailing quotes
        doc.delete( rev=etag )

    def __eq__( self, other ) :
        if not isinstance( other, Database ) : return False
        return True if self is other else False

    def __repr__( self ) :
        return '<%s %r>' % (type(self).__name__, self.dbname)

    def __contains__( self, docid ) :
        """Return whether the document identified by ``docid`` is present in
        the database.
        """
        try :
            doc = Document( self, docid )
            doc.head()
            return True
        except :
            return False

    #---- API methods for database

    def ispresent( self ) :
        """Return a boolean, on database availability in the server."""
        try :
            return bool( self() )
        except :
            return False

    def changes( self, hthdrs={}, **query ) :
        """Obtain a list of changes done to the database. This can be
        used to monitor for update and modifications to the database for post
        processing or synchronization. Returns JSON converted changes objects,
        and the last update sequence number, as returned by CouchDB. Refer to
        ``GET /<db>/_changes`` API for more information.

        query parameters,

        ``feed``,
            Type of feed, longpoll | continuous | normal
        ``filter``,
            Filter function from a design document to get updates.
        ``heartbeat``,
            Period in milliseconds, after which an empty line is sent during
            longpoll or continuous.
        ``include_docs``,
            Must be 'true' to include the document with the result.
        ``limit``,
            Maximum number of rows to return.
        ``since``,
            Start the results from the specified sequence number.
        ``timeout``,
            Maximum period in milliseconds to wait before the response is sent.

        Admin-prev No
        """
        conn, paths = self.conn, ( self.paths + ['_changes'] )
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _changes( conn, paths, hthdrs=hthdrs, **query )
        return d

    def compact( self, designdoc=None, hthdrs={} ) :
        """Request compaction for this database. Compaction compresses the
        disk database file by performing the following operations.

        * Writes a new version of the database file, removing any unused
        sections from the new version during write. Because a new file is
        temporary created for this purpose, you will need twice the current
        storage space of the specified database in order for the compaction
        routine to complete.
        * Removes old revisions of documents from the database, up to the
        per-database limit specified by the _revs_limit database
        parameter.
         
        Alternatively, you can specify the ``designdoc`` key-word argument to
        compact the view indexes associated with the specified design
        document. You can use this in place of the full database compaction if
        you know a specific set of view indexes have been affected by a recent
        database change.

        Return JSON converted object as returned by CouchDB, refer to 
        ``POST /<db>/_compact`` and ``POST /<db>/_compact>/<designdoc>`` for
        more information.

        Admin-prev, Yes
        """
        if designdoc == None :
            conn, paths = self.conn, (self.paths + ['_compact']) 
        else :
            conn, paths = self.conn, (self.paths + ['_compact',designdoc])
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _compact( conn, paths, hthdrs=hthdrs )
        return d

    def viewcleanup( self, hthdrs={} ) :
        """Clean-up the cached view output on the disk.

        Admin-prev, Yes
        """
        conn, paths = self.conn, (self.paths + ['_view_cleanup'])
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _view_cleanup( conn, paths, hthdrs=hthdrs )
        return d

    def ensurefullcommit( self, hthdrs={} ) :
        """Commit recent changes to disk. You should call this if you want to
        ensure that recent changes have been written. Return JSON converted
        object as returned by CouchDB.

        Admin-prev, No
        """
        conn, paths = self.conn, (self.paths + ['_ensure_full_commit'])
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _ensure_full_commit( conn, paths, hthdrs=hthdrs )
        return d

    def bulkdocs( self, docs=[], atomic=False, hthdrs={} ) :
        """Bulk document API allows you to create and update multiple
        documents at the same time within a single request. The basic
        operation is similar to creating or updating a single document, except
        that you batch the document structure and information. When
        creating new documents the document ID is optional. For updating
        existing documents, you must provide the document ID, revision
        information, and new document values.

        You can optionally delete documents during a bulk update by adding the
        `_deleted` field with a value of true to each docment ID/revision
        combination within the submitted JSON structure.

        `docs` contains a list of :class:`Document` instances or dictionaries.
        In case of :class:`Document` instance, the object instance will be
        updated with the new `revision` number.

        To perform document updates and inserts atomically, pass `atomic`
        keyword as True.

        Returns JSON converted return value for CouchDB API /db/_bulk_docs
        
        Admin-prev, No
        """
        conn, paths = self.conn, (self.paths + ['_bulk_docs'])
        h = conn.mixinhdrs( self.hthdrs, hthdrs )
        docs_ = []
        for doc in docs :
            if isinstance(doc, dict) :
                docs_.append(doc)
            elif isinstance(doc, Document) :
                docs_.append( dict( doc.item() ))
            else :
                raise CouchPyError( 'bulk docs contains unknown element' )
        s, h, d = _bulk_docs(conn, docs_, atomic=atomic, paths=paths, hthdrs=h)
        return d

    def bulkdelete( self, docs=[], atomic=False, hthdrs={} ) :
        """Same as :func:`Database.bulkdocs` except that the `_delete`
        property in each document will be set to True, thus deleting the
        documents from the database. To delete the documents in atomic
        fashion, pass keyword parameter `atomic` as True.

        Returns JSON converted return value for CouchDB API /db/_bulk_docs
        """
        docs_ = [ doc.setdefault( '_deleted', True ) for doc in docs ]
        return self.bulkdocs( docs, atomic=atomic, hthdrs=hthdrs )

    def tempview( self, designdoc, hthdrs={}, **query ) :
        """Create (and execute) a temporary view based on the view function
        supplied in the JSON request. This API accepts the same query
        parameters supported by _view API.  Returns JSON converted object as
        returned by CouchdB. Refer to ``POST /<db>/_temp_view`` and
        ``GET  /<db>/_design/<design-doc>/_view/<view-name>``
        sections in CouchDB API reference manual for more information.

        Admin-prev, Yes
        """
        conn, paths = self.conn, (self.paths + ['_temp_view'])
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _temp_view( conn, designdoc, paths, hthdrs=hthdrs, **query )
        return d

    def purge( self, docs, revs=None, hthdrs={} ) :
        """A database purge permanently removes the references to deleted
        documents from the database. Deleting a document within CouchDB
        does not actually remove the document from the database, instead,
        the document is marked as a deleted (and a new revision is created).
        This is to ensure that deleted documents are replicated to other
        databases as having been deleted.
        The purging of old documents is not replicated to other databases.
        Purging documents does not remove the space used by them on disk.
        To reclaim disk space, you should run a database compact operation.

        Either pass a JSON convertible object that will be directly sent as
        request body or, pass a list of documents where each element of the
        list can be a document dictionary or :class:`Document` object.

        Returns JSON converted object as returned by CouchDB API /db/_purge

        >>> d = { "FishStew" : [ "17-b3eb5ac6fbaef4428d712e66483dcb79" ] }
        >>> db.purge( d )
        >>> docs = db.all_docs()
        >>> db.purge( docs )

        Admin-prev Yes
        """
        conn, paths = self.conn, (self.paths + ['_purge'])
        if isinstance(docs, dict) :
            body = docs
        else :
            body = {}
            [ body.setdefault(doc['_id'], []).append( doc['_rev'] )
              for doc in docs ]
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _purge( conn, body, paths, hthdrs=hthdrs )
        return d

    def all_docs( self, keys=None, hthdrs={}, _q={}, **params ) :
        """Return a JSON structure of all of the documents in a given database.
        The information is returned as a JSON structure containing
        meta information about the return structure, and the list documents
        and basic contents, consisting the ID, revision and key. The key is
        generated from the document ID. Refer to ``POST /<db>/_all_docs`` from
        CouchDB API manual for more information.

        If optional key-word argument `keys` is passed, specifying a list of
        document ids, only those documents will be fetched and returned.

        query parameters are similar to that of views.

        ``descending``,
            Return the documents in descending by key order.
        ``endkey``,
            Stop returning records when the specified key is reached.
        ``endkey_docid``,
            Stop returning records when the specified document ID is reached.
        ``group``,
            Group the results using the reduce function to a group or single
            row.
        ``group_level``,
            Description Specify the group level to be used.
        ``include_docs``,
            Include the full content of the documents in the response.
        ``inclusive_end``,
            Specifies whether the specified end key should be included in the
            result.
        ``key``,
            Return only documents that match the specified key.
        ``limit``,
            Limit the number of the returned documents to the specified
            number.
        ``reduce``,
            Use the reduction function.
        ``skip``,
            Skip this number of records before starting to return the results.
        ``stale``,
            Allow the results from a stale view to be used.
        ``startkey``,
            Return records starting with the specified key
        ``startkey_docid``,
            Return records starting with the specified document ID.
        ``update_seq``,
            Include the update sequence in the generated results,

        Alternately, query parameters can be passed as a dictionary or Query
        object to key-word argument ``_q``.

        The 'skip' option should only be used with small values, as skipping a
        large range of documents this way is inefficient

        Admin-prev, No
        """
        conn, paths = self.conn, (self.paths + ['_all_docs'])
        q = _q if isinstance(_q, Query) else Query( q=_q, **params )
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _all_docs( conn, keys=keys, paths=paths, hthdrs=hthdrs, q=q )
        return d

    def missingrevs( self ) :
        """TBD : To be implemented"""

    def revsdiff( self ) :
        """TBD : To be implemented"""

    def security( self, security=None, hthdrs={} ) :
        """Get or Set the current secrity object for this database. The
        security object consists of two compulsory elements, 
            `admins` and `readers`,
        which are used to specify the list of users and/or roles that
        have `admin` and `reader` rights to the database respectively. Any
        additional fields in the security object are optional. The entire
        security object is made available to validation and other internal
        functions, so that the database can control and limit functionality.

        To set current security object, pass them as key-word argument,
        ``security``,
            Security object contains admins and readers
        Return the current security object

        Admin-prev, Yes for setting the security object
        """
        conn, paths = self.conn, (self.paths + ['_security'])
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _security( conn, paths, security=security, hthdrs=hthdrs )
        return d

    def revslimit( self, limit=None, hthdrs={} ) :
        """Get or Set the current revs_limit (revision limit) for database.
        To set revs_limit, pass the value as key-word argument ``limit``

        Admin-prev, Yes for setting the revision limit
        """
        conn, paths = self.conn, (self.paths + ['_revs_limit'])
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _revs_limit( conn, paths, limit=limit, hthdrs=hthdrs )
        return d

    def Document( self, doc, *args, **kwargs ):
        return Document( self, doc, *args, **kwargs )

    def LocalDocument( self, doc, *args, **kwargs ):
        return LocalDocument( self, doc, *args, **kwargs )

    def DesignDocument( self, doc, *args, **kwargs ):
        return DesignDocument( self, doc, *args, **kwargs )

    #---- Properties

    _committed_update_seq = lambda self : self()['committed_update_seq']
    _compact_running      = lambda self : self()['compact_running']
    _disk_format_version  = lambda self : self()['disk_format_version']
    _disk_size            = lambda self : self()['disk_size']
    _doc_count            = lambda self : self()['doc_count']
    _doc_del_count        = lambda self : self()['doc_del_count']
    _instance_start_time  = lambda self : self()['instance_start_time']
    _purge_seq            = lambda self : self()['purge_seq']
    _update_seq           = lambda self : self()['update_seq']

    #---- Database Information
    info                 = property( lambda self : self._info or self() )
    #---- as attributes to this instance.
    committed_update_seq = property( _committed_update_seq )
    compact_running      = property( _compact_running )
    disk_format_version  = property( _disk_format_version )
    disk_size            = property( _disk_size )
    doc_count            = property( _doc_count )
    doc_del_count        = property( _doc_del_count )
    instance_start_time  = property( _instance_start_time )
    purge_seq            = property( _purge_seq )
    update_seq           = property( _update_seq )

    #---- Other properties
    singleton_docs       = property( lambda self : Database._singleton_docs[self.dbname] )

    def put( self, hthdrs={} ) :
        """Create a new database, corresponding to this instance. The database
        name must be adhere to the following rules.
        * Name must begin with a lowercase letter
        * Contains lowercase characters (a-z)
        * Contains digits (0-9)
        * Contains any of the characters _, $, (, ), +, -, and /

        Return, Database object

        Admin-prev Yes
        """
        conn, paths = self.conn, self.paths
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _putdb( conn, paths, hthdrs=hthdrs )
        if d is None :
            return None
        return self

    def delete( self, hthdrs={} ) :
        """Delete the database from the server, and all the documents and
        attachments contained within it.

        Return, JSON converted object as returned by couchDB.

        Admin-prev Yes
        """
        conn, paths = self.conn, self.paths
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _deletedb( conn, paths, hthdrs=hthdrs )
        return None

    def fetch( self ) :
        """Call to this method will bulk fetch all the active document that are
        yet to be fetched from the server.
        """
        activedocs = self.singleton_docs['active'].values()
        fetchdocs  = filter( lambda d : d._x_fetched == False, activedocs )
        fetchids   = map( lambda d : d._id, fetchdocs )
        result     = self.all_docs( keys=fetchids, include_docs=True )
        freshdocs  = dict([ (d['id'], d['doc']) for d in results['rows'] ])
        # Invoke `_onfetch` callback for documents that were successfully
        # commited
        [ doc._onfetch( freshdocs[ doc._id ] ) for doc in fetchdocs ]
        return result

    def commit( self ):
        """Call to this method will bulk commit all the active documents that
        are dirtied.
        """
        activedocs = self.singleton_docs['active'].values()
        dirtydocs  = filter( lambda d : d._x_dirtied, activedocs )
        result     = self.bulkdocs( dirtydocs )
        # Invoke `_oncommit` callback for documents that were successfully
        # commited
        success = dict([
            (x['id'], x) for x in result if 'id' in x and 'rev' in x
        ])
        [ doc._oncommit( _rev=success[doc._id]['rev'] )
          for doc in dirtydocs if doc._id in success.keys() ]
        return result


    # TODO : Collect all special db names ...
    SPECIAL_DB_NAMES = set([ '_users', ])
    VALID_DB_NAME = re.compile(r'^[a-z][a-z0-9_$()+/-]*$')
    @classmethod
    def validate_dbname( cls, name ) :
        if name in cls.SPECIAL_DB_NAMES :
            return name
        if not cls.VALID_DB_NAME.match( name ) :
            raise InvalidDBname('Invalid database name')
        return name


class DatabaseIterator( BaseIterator ):
    def __init__( self, client, values=[], regexp=None ):
        if regexp :
            c = re.compile( regexp )
            values = filter( c.match, values )
        self.client = client
        BaseIterator.__init__( self, values=values )

    def __iter__( self ):
        return self

    def next( self ):
        if self.values :
            return Database( self.client, self.values.pop(0) )
        raise StopIteration
