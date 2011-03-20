import sys, re
from   copy         import deepcopy

import rest
from   httperror    import *
from   httpc        import HttpSession, ResourceNotFound, OK, CREATED
from   client       import Client
from   doc          import Document

# TODO :
#   1. _changes 'longpoll' and 'continuous' modes are not yet supported.
#   2. The 'skip' option should only be used with small values, as skipping a
#      large range of documents this way is inefficient
#   3. _missing_revs API to be implemented
#   4. _revs_diff API to be implemented

hdr_acceptjs = { 'Accept' : 'application/json' }

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

def _db( conn, paths=[], hthdrs={} ) :
    """GET /<db>"""
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _all_docs( conn, keys=[], paths=[], hthdrs={}, **query ) :
    # query descending=<bool>   endkey=<key>        endkey_docid=<id>
    #       group=<bool>        group_level=<num>   include_docs=<bool>
    #       key=<key>           limit=<num>         inclusive_end=<bool>
    #       reduce=<bool>       skip=<num>          stale='ok'
    #       startkey=<key>      startkey_docid=<id> update_seq=<bool>
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    if keys :
        body = rest.data2json({ 'keys' : keys })
        s, h, d = conn.post( paths, hthdrs, body, _query=query.items() )
    else :
        s, h, d = conn.get( paths, hthdrs, None, _query=query.items() )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)
    

def _bulk_docs( conn, docs, paths=[], atomic=False, hthdrs={} ) :
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

def _changes( conn, paths=[], hthdrs={}, **query ) :
    """GET /<db>/_changes"""
    # query feed=normal | continuous | longpoll
    #       filter=<design-doc>/<func-name>     heartbeat=<milliseconds>
    #       include_docs=<bool>                 limit=<number>
    #       since=<seq-num>                     timeout=<millisecond>
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None, _query=query.items() )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

# This following bedlump function also handles _compact/design-doc API
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

def _ensure_full_commit( conn, paths=[], hthdrs={} ) :
    """POST /<db>/_ensure_full_commit"""
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.post( paths, hthdrs, None )
    if s == OK and d["ok"] == True :
        return s, h, d
    else :
        return (None, None, None)

def _missing_revs( conn, revs=[], paths=[], hthdrs={} ) :
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    body = rest.data2json( revs )
    s, h, d = conn.post( paths, hthdrs, body )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _purge( conn, body, paths=[], hthdrs={} ) :
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    body = rest.data2json( body )
    s, h, d = conn.post( paths, hthdrs, body )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _revs_diff( conn, revs=[], paths=[], hthdrs={} ) :
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    body = rest.data2json( revs )
    s, h, d = conn.post( paths, hthdrs, body )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _revs_limit( conn, paths=[], limit=None, hthdrs={} ) :
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    body = '%s' % limit if limit != None else None
    s, h, d = conn.put( paths, hthdrs, body
              ) if limit != None else conn.get( paths, hthdrs, body )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _security( conn, paths=[], security=None, hthdrs={} ) :
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    body = rest.data2json( security ) if security else None
    s, h, d = conn.put( paths, hthdrs, body
              ) if security != None else conn.get( paths, hthdrs, body )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _temp_view( conn, designdoc, paths=[], hthdrs={}, **query ) :
    # TODO : Same query parameters as that of design-doc views
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    body = rest.data2json( designdoc )
    s, h, d = conn.post( paths, hthdrs, body, _query=query.items() )
    if s == OK :
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

class Database( object ) :

    def __init__( self, client, dbname, **kwargs ) :
        """Interface with database `dbname`"""
        self.dbname = Database.validate_dbname( dbname )
        self.client = client
        self.conn = client.conn
        self.debug = kwargs.pop( 'debug', client.debug )

        self.paths = [ dbname ]
        self.info = {}

    def __call__( self ) :
        """Gets information about the specified database

        Return,
            Returns the information dictionary.
        Admin-Prev,
            No
        """
        s, h, d = _db( self.conn, self.paths )
        self.info = d
        return d

    def __iter__( self ) :
        """Return the IDs of all documents in the database."""
        d = self.docs()
        return iter( map( lambda x : x["id"], d['rows'] ))

    def __getitem__( self, key ) :
        """Fetch the document specified by `key` and return a corresponding
        Document object
        """
        return Document( self, key )

    def __len__(self):
        """Return the number of documents in the database."""
        d = self.docs()
        return d['total_rows']

    def __nonzero__(self):
        """Return whether the database is available."""
        return self.ispresent()

    def __delitem__(self, docid):
        """Remove the document with the specified ID from the database"""
        s, h, d = Document.head( self, docid )
        self.deletedoc( docid, rev=h['Etag'] )

    def __eq__( self, other ) :
        if not isinstance( other, Database ) : return False
        if self.name == other.name : return True
        return False

    def __repr__( self ) :
        return '<%s %r>' % (type(self).__name__, self.name)

    def __contains__( self, docid ) :
        """Return whether the database contains a document with the specified
        docid.

        Return,
            `True` if a document with the id exists, `False` otherwise.
        """
        try :
            Document.head( self, docid )
            return True
        except :
            return False

    def ispresent( self ) :
        try :
            self()
            return True
        except :
            return False

    def changes( self, hthdrs={}, **query ) :
        """Obtains a list of the changes made to the database. This can be
        used to monitor for update and modifications to the database for post
        processing or synchronization.

        query parameters,
        feed,
            Type of feed, longpoll | continous | normal
        filter,
            Filter function from a design document to get updates.
        heartbeat,
            Period after which an empty line is sent during longpoll or
            continuous.
        include_docs,
            Include the document with the result.
        limit,
            Maximum number of rows to return.
        since,
            Start the results from the specified sequence number.
        timeout,
            Maximum period to wait before the response is sent, in
            milliseconds.

        Return,
            JSON converted changes objects, and the last update sequence
            number, as returned by CouchDB
        Admin-prev
            No
        """
        conn, paths = self.conn, self.paths
        paths.append( '_changes' )
        s, h, d = _changes( conn, paths, hthdrs=hthdrs, **query )
        return d

    def compact( self, designdoc=None, hthdrs={} ) :
        """Request compaction of the specified database. Compaction
        compresses the disk database file by performing the following
        operations.
        * Writes a new version of the database file, removing any unused
        sections from the new version during write. Because a new file is
        temporary created for this purpose, you will need twice the current
        storage space of the specified database in order for the compaction
        routine to complete.
        * Removes old revisions of documents from the database, up to the
        per-database limit specified by the _revs_limit database
        parameter.

        Alternatively, you can specify the `designdoc` key-word argument to
        compacts the view indexes associated with the specified design
        document. You can use this in place of the full database compaction if
        you know a specific set of view indexes have been affected by a recent
        database change.

        Return,
            JSON converted object as returned by CouchDB
        Admin-prev
            No
        """
        conn, paths = self.conn, self.paths
        paths = ['_compact'] if designdoc == None else ['_compact',designdoc]
        s, h, d = _compact( conn, paths, hthdrs=hthdrs )
        return d

    def viewcleanup( self, hthdrs-{} ) :
        """Cleans up the cached view output on disk for a given view.

        Returns,
            None,
        Admin-prev,
            Yes
        """
        conn, paths = self.conn, self.paths
        paths.append( '_view_cleanup' )
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
        conn, paths = self.conn, self.paths
        paths.append( '_ensure_full_commit' )
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
        `_deleted` field with a value of true to each docment ID/
        revision combination within the submitted JSON structure.

        Return,
            JSON converted object as returned by CouchDB. It depends on
            whether atomic is True or False. Refer to CouchDB API reference
            manual for more information.
        Admin-prev,
            No
        """
        conn, paths = self.conn, self.paths
        paths.append( '_bulk_docs' )
        s, h, d = _bulk_docs( conn, docs, paths, atomic=atomic,
                              hthdrs=hthdrs )
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
        conn, paths = self.conn, self.paths
        paths.append( '_temp_view' )
        s, h, d = _temp_view( conn, designdoc, paths, hthdrs=hthdrs,
                              **query )
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
        conn, paths = self.conn, self.paths
        paths.append( '_purge' )
        if isinstance(doc, dict) :
            body = rest.data2json(doc)
        else :
            _id = doc._id if isinstance(doc, Document) else doc
            revs = [ doc._rev ] if revs == None else revs
            body = rest.data2json({ _id : revs })
        s, h, d = _purge( conn, body, paths, hthdrs=hthdrs )
        return d

    def docs( self, keys=None, hthdrs={}, **query ) :
        """Returns a JSON structure of all of the documents in a given
        database. The information is returned as a JSON structure containing
        meta information about the return structure, and the list documents
        and basic contents, consisting the ID, revision and key. The key is
        generated from the document ID.

        If optional key-word argument `keys` is passes, specifying a list
        document ids, only those documents will be fetched and returned.

        query parameters,
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

        Admin-prev
            No
        """
        conn, paths = self.conn, self.paths
        paths.append( '_all_docs' )
        s, h, d = _all_docs( conn, keys=keys, paths, hthdrs=hthdrs, **query )
        return d

    def missingrevs( self ) :
        """Not fully defined.
        """

    def revsdiff( self ) :
        """Not fully defined.
        """

    def security( self, obj=None, hthdrs={} ) :
        """Get or Set the current security object.
        If `obj` is specified the security object will be updated. Else, the
        current security object will be returned
        """
        conn, paths = self.conn, self.paths
        paths.append( '_security' )
        s, h, d = _security( conn, paths, security=obj, hthdrs=hthdrs )
        return d

    def revslimit( self, limit=None, hthdrs={} ) :
        """Gets or Set the current revs_limit (revision limit) for database.
        """
        conn, paths = self.conn, self.paths
        paths.append( '_revs_limit' )
        s, h, d = _revs_limit( conn, paths, limit=limit, hthdrs=hthdrs )
        return d

    def createdoc( self, docs=[], filepaths=[], hthdrs={}, **query ) :
        """Create a one or more document in this database. Optionally provide
        a list of file-paths to be added as attachments to the document, HTTP
        headers, and query parameters,

        query parameters,
        batch,
            if specified 'ok', allow document store request to be batched with
            other

        Return,
            Document object
        Admin-prev,
            No
        """
        if isinstance(docs, (list, tuple)) :
            return [ Document.create( self, doc, hthdrs=hthdrs, **query )
                     for doc in docs ]
        else :
            return Document.create( self, docs, attachfiles=filepaths,
                                    hthdrs=hthdrs, **query )

    def deletedoc( self, docs=[], rev=None, hthdrs={} ) :
        """Deletes one or more documents from the database, and all the
        attachments contained within it. When deleting single document,
        kqy-word argument `rev` (current revision of the document) should be
        specified. When deleting multiple documents, `docs` must be a list of
        tuples, (docid, document-revision)

        Return,
            None
        Admin-Prev,
            No
        """
        if isinstance(docs, (list, tuple)) :
            [ Document.delete( self, doc, hthdrs=hthdrs, rev=rev 
              ) for doc, rev in docs ]
        else :
            Document.delete( self, docs, hthdrs=hthdrs, rev=rev )
        return None

    @classmethod
    def create( client, dbname, hthdrs={} ) :
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
        s, h, d = _createdb( client.conn, self.paths, hthdrs=hthdrs )
        if s == OK and d["ok"] == True :
            return Database( client, dbname, hthdrs=hthdrs )
        else :
            return None

    @classmethod
    def delete( client, dbname, hthdrs={} ) :
        """Deletes the specified database, and all the documents and
        attachments contained within it.

        Return,
            JSON converted object as returned by couchDB.
        Admin-prev
            No
        """
        s, h, d = _deletedb( client.conn, self.paths, hthdrs=hthdrs )
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
