"""Database definition for accessing CouchDB database. An instance of
:class:`Database` class corresponds to a single database in CouchDB server,
the object can be used to interface with it. Aside from that,
:class:`Database` objects provide pythonified way of accessing it.

Create a client object,

>>> c = Client()
>>> db = c.create('dbname_first') # Create
"""
import sys, re
from   copy         import deepcopy

import couchpy.rest
from   couchpy.httperror  import *
from   couchpy.httpc      import HttpSession, ResourceNotFound, OK, CREATED
from   couchpy.client     import Client
from   couchpy.doc        import Document, LocalDocument
from   couchpy.designdoc  import DesignDocument
from   couchpy.attachment import Attachment
from   couchpy.query      import Query

# GOTCHA :
#   1. The 'skip' option should only be used with small values, as skipping a
#      large range of documents this way is inefficient
#
# TODO :
#   1. _changes, 'longpoll' and 'continuous' modes are not yet supported.
#   2. _bulk_docs, needs to be updated.
#   3. _missing_revs API to be implemented
#   4. _revs_diff API to be implemented
#   5. _security API to be implemented
#   6. Should we provide attachment facilities for local docs ?

hdr_acceptjs = { 'Accept' : 'application/json' }

def _db( conn, paths=[], hthdrs={} ) :
    """GET /<db>"""
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _createdb( conn, paths=[], hthdrs={} ) :
    """PUT /<db>"""
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.put( paths, hthdrs, None )
    if s == OK and d['ok'] == True :
        return s, h, d
    else :
        return (None, None, None)

def _deletedb( conn, paths=[], hthdrs={} ) :
    """DELETE /<db>"""
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.delete( paths, hthdrs, None )
    if s == OK and d['ok'] == True :
        return s, h, d
    else :
        return (None, None, None)

def _changes( conn, paths=[], hthdrs={}, **query ) :
    """GET /<db>/_changes
    query,
        feed=normal | continuous | longpoll
        filter=<design-doc>/<func-name>     heartbeat=<milliseconds>
        include_docs=<bool>                 limit=<number>
        since=<seq-num>                     timeout=<millisecond>
    """
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None, _query=query.items() )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _compact( conn, paths=[], hthdrs={} ) :
    """POST /<db>/_compact;
       POST /<db>/_compact>/<designdoc>"""
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.post( paths, hthdrs, None )
    if s == OK and d["ok"] == True :
        return s, h, d
    else :
        return (None, None, None)

def _view_cleanup( conn, paths=[], hthdrs={} ) :
    """POST /<db>/_view_cleanup"""
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.post( paths, hthdrs, None )
    if s == OK and d["ok"] == True :
        return s, h, d
    else :
        return (None, None, None)

def _ensure_full_commit( conn, paths=[], hthdrs={} ) :
    """POST /<db>/_ensure_full_commit"""
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.post( paths, hthdrs, None )
    if s == OK and d["ok"] == True :
        return s, h, d
    else :
        return (None, None, None)

def _bulk_docs( conn, docs, atomic=False, paths=[], hthdrs={} ) :
    """POST /<db>/_bulk_docs"""
    docs = {
        'all_or_nothing' : atomic,
        'docs' : docs,
    }
    body = rest.data2json( docs )
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.post( paths, hthdrs, body )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _temp_view( conn, designdoc, paths=[], hthdrs={}, **query ) :
    """POST /<db>/_temp_view
    query,
        Same query parameters as that of design-doc views
    """
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    body = rest.data2json( designdoc )
    s, h, d = conn.post( paths, hthdrs, body, _query=query.items() )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _purge( conn, body, paths=[], hthdrs={} ) :
    """POST /<db>/_purge"""
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    body = rest.data2json( body )
    s, h, d = conn.post( paths, hthdrs, body )
    if s == OK :
        return s, h, d
    else :
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
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    if keys == None :
        s, h, d = conn.get( paths, hthdrs, None, _query=q.items() )
    else :
        body = rest.data2json({ 'keys' : keys })
        s, h, d = conn.post( paths, hthdrs, body, _query=q.items() )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _missing_revs( conn, revs=[], paths=[], hthdrs={} ) :
    """TODO : To be implemented"""
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    body = rest.data2json( revs )
    s, h, d = conn.post( paths, hthdrs, body )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _revs_diff( conn, revs=[], paths=[], hthdrs={} ) :
    """TODO : To be implemented"""
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    body = rest.data2json( revs )
    s, h, d = conn.post( paths, hthdrs, body )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _security( conn, paths=[], security=None, hthdrs={} ) :
    """TODO : To be implemented"""
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    body = rest.data2json( security ) if security else None
    s, h, d = conn.put( paths, hthdrs, body
              ) if security != None else conn.get( paths, hthdrs, body )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _revs_limit( conn, paths=[], limit=None, hthdrs={} ) :
    """
    GET /<db>/_revs_limit       if limit is None
    PUT /<db>/_revs_limit       if limit is an integer value
    """
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    body = '%s' % limit if limit != None else None
    if limit == None :
        s, h, d = conn.get( paths, hthdrs, body )
    else :
        s, h, d = conn.put( paths, hthdrs, body )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)



class Database( object ) :

    def __init__( self, client, dbname, **kwargs ) :
        """Instantiate the database object corresponding to ``dbname`` in
        CouchdDB server provided by ``client``.

        Optional arguments,

        ``debug``, 
            for enhanced logging
        """
        self.client = client
        self.dbname = Database.validate_dbname( dbname )
        self.debug = kwargs.pop( 'debug', client.debug )

        self.conn = client.conn
        self.paths = client.paths + [ dbname ]
        self.info = {}

    def __call__( self ) :
        """Returns information about this database, refer to ``GET /db`` API
        from CouchDB to know the structure of information. 

        Admin-Prev, No
        """
        s, h, d = _db( self.conn, self.paths )
        self.info = d
        return d

    def __iter__( self ) :
        """Iterate over all document IDs in this database. For every
        iteration, `_id` value will be yielded"""
        d = self.docs()
        return iter( map( lambda x : x['id'], d['rows'] ))

    def __getitem__( self, key ) :
        """Fetch the latest revision of the document specified by ``key`` and
        return a corresponding :class:`couchpy.doc.Document` object. To avoid
        fetching document from the database, directly instantiate the Document
        object as below,
        
        >>> Document( db, _id, fetch=False )

        Where `db` is :class:`Database` object and _id is document-id
        """
        return Document( self, key )

    def __len__(self ) :
        """Return number of documents in the database."""
        d = self.docs()
        return d['total_rows']

    def __nonzero__(self):
        """Return a boolean, on database availability in the server. Python
        way of bool-check for :func:`Database.ispresent`"""
        return self.ispresent()

    def __delitem__(self, docid) :
        """Remove the document specified by ``docid`` from database"""
        s, h, d = Document.head( self, docid )
        self.deletedoc( docid, rev=h['Etag'] )

    def __eq__( self, other ) :
        if not isinstance( other, Database ) : return False
        if self.name == other.name : return True
        return False

    def __repr__( self ) :
        return '<%s %r>' % (type(self).__name__, self.name)

    def __contains__( self, docid ) :
        """Return whether the document identified by ``docid`` is present in
        the database.
        """
        try :
            Document.head( self, docid )
            return True
        except :
            return False

    def ispresent( self ) :
        """Return a boolean, on database availability in the server."""
        try :
            self()
            return True
        except :
            return False

    def changes( self, hthdrs={}, **query ) :
        """Obtains a list of changes done to the database. This can be
        used to monitor for update and modifications to the database for post
        processing or synchronization. Returns JSON converted changes objects,
        and the last update sequence number, as returned by CouchDB. Refer to
        ``GET /<db>/_changes`` API for more information.

        query parameters,

        ``feed``,
            Type of feed, longpoll | continous | normal
        ``filter``,
            Filter function from a design document to get updates.
        ``heartbeat``,
            Period after which an empty line is sent during longpoll or
            continuous.
        ``include_docs``,
            Include the document with the result.
        ``limit``,
            Maximum number of rows to return.
        ``since``,
            Start the results from the specified sequence number.
        ``timeout``,
            Maximum period to wait before the response is sent, in
            milliseconds.

        Admin-prev
            No
        """
        conn, paths = self.conn, ( self.paths + ['_changes'] )
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

        Admin-prev, No
        """
        conn, paths = ( self.conn, (self.paths + ['_compact']) 
                      ) if designdoc == None else ( 
                        self.conn, (self.paths + ['_compact',designdoc])
                      )
        s, h, d = _compact( conn, paths, hthdrs=hthdrs )
        return d

    def viewcleanup( self, hthdrs={} ) :
        """Clean-up the cached view output on the disk.

        Admin-prev, Yes
        """
        conn, paths = self.conn, (self.paths + ['_view_cleanup'])
        s, h, d = _view_cleanup( conn, paths, hthdrs=hthdrs )
        return None

    def ensurefullcommit( self, hthdrs={} ) :
        """Commits any recent changes to the specified database to disk. You
        should call this if you want to ensure that recent changes have been
        written.
        
        Returns,
            JSON converted object as returned by CouchDB
        Admin-prev
            No
        """
        conn, paths = self.conn, (self.paths + ['_ensure_full_commit'])
        s, h, d = _ensure_full_commit( conn, paths, hthdrs=hthdrs )
        return d

    def bulkdocs( self, docs=[], atomic=False, hthdrs={}, ) :
        """The bulk document API allows you to create and update multiple
        documents at the same time within a single request. The basic
        operation is similar to creating or updating a single document, except
        that you batch the document structure and information. When
        creating new documents the document ID is optional. For updating
        existing documents, you must provide the document ID, revision
        information, and new document values.
        You can optionally delete documents during a bulk update by adding the
        `_deleted` field with a value of true to each docment ID/revision
        combination within the submitted JSON structure.

        Return,
            JSON converted object as returned by CouchDB. It depends on
            whether atomic is True or False. Refer to CouchDB API reference
            manual for more information.
        Admin-prev,
            No
        """
        conn, paths, h = self.conn, (self.paths + ['_bulk_docs']), hthdrs
        s, h, d = _bulk_docs( conn, docs, atomic=atomic, paths=paths, hthdrs=h )
        return d

    def tempview( self, designdoc, hthdrs={}, **query ) :
        """Creates (and executes) a temporary view based on the view function
        supplied in the JSON request.

        query parameter,
        Same as that of permanent view API

        Return,
            JSON converted object as returned by CouchdB
        Admin-prev,
            Yes
        """
        conn, paths = self.conn, (self.paths + ['_temp_view'])
        s, h, d = _temp_view( conn, designdoc, paths, hthdrs=hthdrs, **query )
        return d

    def purge( self, doc, revs=None, hthdrs={} ) :
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
        request body or, pass in document-id and a corresponding revision list
        or just a Document object.

        Return,
            JSON converted object as returned by CouchDB
        Admin-prev
            No
        """
        conn, paths = self.conn, (self.paths + ['_purge'])
        if isinstance(doc, dict) :
            body = rest.data2json(doc)
        else :
            _id = doc._id if isinstance(doc, Document) else doc
            revs = [ doc._rev ] if revs == None else revs
            body = rest.data2json({ _id : revs })
        s, h, d = _purge( conn, body, paths, hthdrs=hthdrs )
        return d

    def docs( self, keys=None, hthdrs={}, _q={}, **query ) :
        """Returns a JSON structure of all of the documents in a given
        database. The information is returned as a JSON structure containing
        meta information about the return structure, and the list documents
        and basic contents, consisting the ID, revision and key. The key is
        generated from the document ID.

        If optional key-word argument `keys` is passes, specifying a list of
        document ids, only those documents will be fetched and returned.

        query parameters, if keys=None
        descending,
            Return the documents in descending by key order.
        endkey,
            Stop returning records when the specified key is reached.
        endkey_docid,
            Stop returning records when the specified document ID is reached.
        group,
            Group the results using the reduce function to a group or single
            row.
        group_level,
            Description Specify the group level to be used.
        include_docs,
            Include the full content of the documents in the response.
        inclusive_end,
            Specifies whether the specified end key should be included in the
            result.
        key,
            Return only documents that match the specified key.
        limit,
            Limit the number of the returned documents to the specified
            number.
        reduce,
            Use the reduction function.
        skip,
            Skip this number of records before starting to return the results.
        stale,
            Allow the results from a stale view to be used.
        startkey,
            Return records starting with the specified key
        startkey_docid,
            Return records starting with the specified document ID.
        update_seq,
            Include the update sequence in the generated results,

        Alternately, query parameters can be passes as a dictionary or Query
        object to key-word argument `_q`.

        Admin-prev
            No
        """
        conn, paths, h = self.conn, (self.paths + ['_all_docs']), hthdrs
        q = dict( _q.items() )
        q.update( query )
        s, h, d = _all_docs( conn, keys=keys, paths=paths, hthdrs=h, q=q )
        return d

    def missingrevs( self ) :
        """TODO : To be implemented"""

    def revsdiff( self ) :
        """TODO : To be implemented"""

    def security( self, obj=None, hthdrs={} ) :
        """TODO : To be implemented"""

    def revslimit( self, limit=None, hthdrs={} ) :
        """Gets or Set the current revs_limit (revision limit) for database."""
        conn, paths = self.conn, (self.paths + ['_revs_limit'])
        s, h, d = _revs_limit( conn, paths, limit=limit, hthdrs=hthdrs )
        return d

    def createdoc( self, docs=None, localdocs=None, designdocs=None,
                   filepaths=[], hthdrs={}, **query ) :
        """Create one or more document in this database. Documents can be of
            Normal documents
            Local documents
            Design document
        Optionally provide a list of file-paths to be added as attachments to
        the document, HTTP headers, and query parameters,

        query parameters, for normal documents / local documents
        batch,
            if specified 'ok', allow document store request to be batched with
            other

        Return,
            Document object (or) LocalDocument object (or)
            DesignDocument object
        Admin-prev,
            No
        """
        h, q, f = hthdrs, query, filepaths
        r = None
        if docs != None and isinstance(docs, (list, tuple)) :
            r = [ Document.create(self, doc, hthdrs=h, **q) for doc in docs ]
        elif docs != None :
            r = Document.create( self, docs, attachfiles=f, hthdrs=h, **q )
        elif localdocs != None and isinstance(localdocs, (list, tuple)) :
            r = [ LocalDocument.create(self, doc, hthdrs=h, **q) 
                  for doc in localdocs ]
        elif localdocs != None :
            r = LocalDocument.create( self, localdocs, hthdrs=h, **q )
        elif designdocs != None and isinstance(designdocs, (list,tuple)) :
            r = [ DesignDocument.create(self, doc, hthdrs=h) 
                  for doc in designdocs ]
        elif designdocs != None :
            r = DesignDocument.create(self, doc, hthdrs=h)
        return r

    def deletedoc( self, docs=None, localdocs=None, designdocs=None,
                   rev=None, hthdrs={} ) :
        """Deletes one or more documents from the database and all the
        attachments contained in the document(s). Documents can be
        of,
            Normal documents
            Local documents,
            Design documents
        and all the
        When deleting single document, key-word argument `rev` (current revision
        of the document) should be specified.
        When deleting multiple documents, `docs` must be a list of tuples,
        (docid, document-revision)

        Return,
            None
        Admin-Prev,
            No
        """
        h = hthdrs
        if docs != None and isinstance(docs, (list, tuple)) :
            [Document.delete(self, doc, hthdrs=h, rev=rev) for doc,rev in docs]
        elif docs != None :
            Document.delete( self, docs, hthdrs=h, rev=rev )
        elif localdocs != None and isinstance(docs, (list, tuple)) :
            [ LocalDocument.delete(self, doc, hthdrs=h, rev=rev) 
              for doc, rev in localdocs]
        elif localdocs != None :
            LocalDocument.delete( self, localdocs, hthdrs=h, rev=rev )
        elif designdocs != None and isinstance(designdocs, (list,tuple)) :
            [ DesignDocument.delete(self, doc, hthdrs=h, rev=rev) 
              for doc, rev in designdocs ]
        elif designdocs != None :
            DesignDocument.delete(self, designdocs, hthdrs=h, rev=rev)
        return None

    def localdocs( self, keys=None, hthdrs={}, **query ) :
        """Return a list of local documentation ids, internally, query
        parameters,
            startkey="_local/",
            endkey="_local0"
        will be used to fetch the local documents. Other query parameters can
        be passed as key-word arguments.
        """
        q = Query( startkey="_local/", endkey="_local0" )
        q.update( **query )
        d = self.docs( keys=keys, hthdrs=hthdrs, _q=q )
        return map( lambda x : x['id'], d['rows'] )

    def designdocs( self, keys=None, hthdrs={}, **query ) :
        """Return a list of design documentation ids, internally, query
        parameters,
            startkey="_design/",
            endkey="_design0"
        will be used to fetch the design documents. Other query parameters can
        be passed as key-word arguments.
        """
        q = Query( startkey="_design/", endkey="_design0" )
        q.update( **query )
        d = self.docs( keys=keys, hthdrs=hthdrs, _q=q )
        return map( lambda x : x['id'], d['rows'] )

    def copydoc( self, docid, rev, toid, asrev=None, hthdrs={} ) :
        """Copy the specified document `docid` from `revision` to document
        `toid` as revision `asrev`. `docid` should specify the source document
        id, based on the id.

        Return,
            On success, destination's document object, which can be of the
            time Document or LocalDocument or DesignDocument, else None
        Admin-prev,
            No
        """
        h = hthdrs
        if docid.startswith( '_local' ) :
            d = LocalDocument.copy( self, docid, toid, asrev=asrev, hthdrs=h,
                                    rev=rev )
        elif docid.startswith( '_design' ) :
            d = DesignDocument.copy( self, docid, toid, asrev=asrev, hthdrs=h,
                                     rev=rev )
        else :
            d = Document.copy( self, docid, toid, asrev=asrev, hthdrs=h,
                               rev=rev )
        return d

    _committed_update_seq = lambda self : self()['committed_update_seq']
    _compact_running      = lambda self : self()['compact_running']
    _disk_format_version  = lambda self : self()['disk_format_version']
    _disk_size            = lambda self : self()['disk_size']
    _doc_count            = lambda self : self()['doc_count']
    _doc_del_count        = lambda self : self()['doc_del_count']
    _instance_start_time  = lambda self : self()['instance_start_time']
    _purge_seq            = lambda self : self()['purge_seq']
    _update_seq           = lambda self : self()['update_seq']

    committed_update_seq = property( _committed_update_seq )
    compact_running      = property( _compact_running )
    disk_format_version  = property( _disk_format_version )
    disk_size            = property( _disk_size )
    doc_count            = property( _doc_count )
    doc_del_count        = property( _doc_del_count )
    instance_start_time  = property( _instance_start_time )
    purge_seq            = property( _purge_seq )
    update_seq           = property( _update_seq )

    @classmethod
    def create( cls, client, dbname, hthdrs={} ) :
        """
        Creates a new database. The database name must be composed of one or
        more of the following characters.
        * Lowercase characters (a-z)
        * Name must begin with a lowercase letter
        * Digits (0-9)
        * Any of the characters _, $, (, ), +, -, and /

        Return,
            Database object
        Admin-prev
            No
        """
        conn, paths = client.conn, (client.paths + [ dbname ])
        s, h, d = _createdb( conn, paths, hthdrs=hthdrs )
        if s == OK and d["ok"] == True :
            return Database( client, dbname, hthdrs=hthdrs )
        else :
            return None

    @classmethod
    def delete( cls, client, dbname, hthdrs={} ) :
        """Deletes the specified database, and all the documents and
        attachments contained within it.

        Return,
            JSON converted object as returned by couchDB.
        Admin-prev
            No
        """
        conn, paths = client.conn, (client.paths + [dbname])
        s, h, d = _deletedb( conn, paths, hthdrs=hthdrs )
        return d

    # TODO : Collect all special db names ...
    SPECIAL_DB_NAMES = set([ '_users', ])
    VALID_DB_NAME = re.compile(r'^[a-z][a-z0-9_$()+-/]*$')
    @classmethod
    def validate_dbname( cls, name ) :
        if name in cls.SPECIAL_DB_NAMES :
            return name
        if not cls.VALID_DB_NAME.match( name ) :
            raise InvalidDBname('Invalid database name')
        return name
