import sys, re, json, time
from   os.path            import basename
from   copy               import deepcopy
from   StringIO           import StringIO
from   mimetypes          import guess_type
import base64

from   httperror          import *
from   httpc              import HttpSession, ResourceNotFound, OK, CREATED
from   couchpy            import CouchPyError
from   couchpy.attachment import Attachment

# TODO :
#   1. Batch mode POST / PUT should have a verification system built into it.
#   2. Attachments allowed in local documents ???

log = configlog( __name__ )

""" Document Structure
{
    '_id' : <unique id>
    '_rev' : <current-revno>
    '_attachments' : {
        <filename> : {      // For updating attachments
            'content_type' : '<content-type>'
            'data' : '<base64-encoded data>'
        },
        <filename> : {      // Attachment stubs in DB
            'content_type' : '<content-type>'
            'length' : '<len>'
            'revpos' : '<doc-revision>'
            'stub'   : '<bool>'
        },
        ....
    '_deleted' : true,
    '_conflict' : true,
    }
"""

hdr_acceptjs = { 'Accept' : 'application/json' }

def _createdoc( conn, doc, paths=[], hthdrs={}, **query ) :
    """POST /<db>/<doc>
    query,
        batch='ok'
    """
    body = rest.data2json( doc )
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.post( paths, hthdrs, body, _query=query.items() )
    if s == OK and d['ok'] == True :
        return s, h, d
    else :
        return (None, None, None)

def _readdoc( conn, paths=[], hthdrs={}, **query ) :
    """
    GET /<db>/<doc>
    GET /<db>/_local/<doc>
    query,
        rev=<_rev>, revs=<'true'>, revs_info=<'true'>
    """
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None, _query=query.items() )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _headdoc( conn, paths=[], hthdrs={}, **query ) :
    """
    HEAD /<db>/<doc>
    HEAD /<db>/_local/<doc>
    query,
        rev=<_rev>, revs=<'true'>, revs_info=<'true'>
    """
    s, h, d = conn.head( paths, hthdrs, None, _query=query.items() )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _updatedoc( conn, doc, paths=[], hthdrs={}, **query ) :
    """
    PUT /<db>/<doc>
    PUT /<db>/_local/<doc>
    query,
        batch='ok'
    """
    if '_id' not in doc :
        raise CouchPyError( '`_id` to be supplied while updating the doc' )
    if '_local' not in paths and '_rev' not in doc :
        raise CouchPyError( '`_rev` to be supplied while updating the doc' )
    body = rest.data2json( doc )
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.put( paths, hthdrs, body, _query=query.items() )
    if s == OK and d['ok'] == True :
        return s, h, d
    else :
        return (None, None, None)

def _deletedoc( conn, doc, paths=[], hthdrs={}, **query ) :
    """
    DELETE /<db>/<doc>
    DELETE /<db>/_local/<doc>
    query,
        rev=<_rev>
    """
    if 'rev' not in query :
        raise CouchPyError( '`rev` to be supplied while deleteing the doc' )
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.delete( paths, hthdrs, None, _query=query.items() )
    if s == OK and d['ok'] == True :
        return s, h, d
    else :
        return (None, None, None)

def _copydoc( conn, paths=[], hthdrs={}, **query ) :
    """
    COPY /<db>/<doc>
    COPY /<db>/_local/<doc>
    query,
        rev=<_srcrev>
    """
    if 'Destination' not in hthdrs :
        raise CouchPyError( '`Destination` header field not supplied' )
    s, h, d = conn.copy( paths, hthdrs, None, _query=query.items() )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)


class Document( object ) :

    def __init__( self, db, doc, fetch=True, hthdrs={}, **query ) :
        """
        Read the document specified by doc, which can be either a dictionary
        containing `_id` key or a string to be interpreted as `_id` from
        database `db`.  If key-word argument `fetch` is passed as False, then
        the document will not be fetched from the database. Optionally accepts
        HTTP headers `hthdrs`. 

        query parameters,

        rev,
            Specify the revision to return
        revs,
            Return a list of the revisions for the document
        revs_info,
            Return a list of detailed revision information for the document

        Return,
            Document object
        Admin-prev,
            No
        """
        # TODO : url-encode _id ???
        self.db = db
        self.conn = db.conn

        id_ = doc if isinstance(doc, basestring) else doc['_id']
        self.paths = db.paths + [ id_ ]
        s, h, d = _readdoc( self.conn, self.paths, hthdrs=hthdrs, **query
                  ) if fetch == True else (None, None, {})
        self.doc = d

        self.revs = None        # Cached object
        self.revs_info = None   # Cached object
        self.client = db.client
        self.debug = db.debug

    def __getitem__(self, key) :
        """Read JSON converted document object like a dictionary"""
        return self.doc.get( key, None )

    def __setitem__(self, key, value) :
        """Update JSON converted document object like a dictionary. Updating
        the document object using this interface will automatically PUT the
        document into the database. However, the following keys cannot be
        updated,
            `_id`, `_rev`
        To refresh the Document object, so that it reflects the
        database document, call the Document object, `doc()`
        
        Return,
            None
        Admin-prev,
            No
        """
        reserved = [ '_id', '_rev' ]
        if key in reserved : return None
        self.doc.update({ key : value })
        s, h, d = _updatedoc( self.conn, self.doc, self.paths )
        self.doc.update({ '_rev' : d['rev'] }) if d else None
        self.revs = None            # Enforce a fetch
        self.revs_info = None       # Enforce a fetch
        return None

    def __delitem__( self, key ) :
        """Python way of deleting the item, you can also use, delitem()
        method
        """
        return self.delitem( key )

    def __iter__(self):
        """Yield a key,value pair for every iteration"""
        return iter( self.doc.items() )

    def __call__( self, hthdrs={}, **query ) :
        """
        If no argument is speficied refresh the document from database.
        Optionally accepts HTTP headers `hthdrs`.

        query parameters,
        rev,
            If specified, and not the same as this Document
            object's revision, create a fresh Document object with the
            document of specified revision read from database.
        revs,
            If True, return JSON converted object containing a list of
            revisions for the document. Structure of the returned object is
            defined by CouchDB
        revs_info,
            If True, return JSON converted object containing extended
            information about all revisions of the document. Structure of the
            returned object is defined by CouchDB.
        Note that, `revs` and `revs_info` object are cached using Etag header
        value.

        Return,
            Based on query parameters
        Admin-prev,
            No
        """
        rev = query.get( 'rev', None )
        revs = query.get( 'revs', None )
        revs_info = query.get( 'revs_info', None )
        conn, paths = self.conn, self.paths

        if rev != None and rev != self.doc['_rev'] :
            return self.__class__( self.db, self.doc, hthdrs=hthdrs, rev=rev )
        elif revs == True :
            q = { 'revs' : 'true' }
            if self.revs :
                s, h, d = _headdoc( conn, paths, hthdrs=hthdrs, **q )
                if s == OK and h['Etag'] == self.doc['_rev'] :
                    return self.revs
            s, h, d = _readdoc( conn, paths, hthdrs=hthdrs, **q )
            self.revs = d
            return self.revs
        elif revs_info == True :
            q = { 'revs_info' : 'true' }
            if self.revs_info :
                s, h, d = _headdoc( conn, paths, hthdrs=hthdrs, **q )
                if s == OK and h['Etag'] == self.doc['_rev'] :
                    return self.revs_info
            s, h, d = _readdoc( conn, paths, hthdrs=hthdrs, **q )
            self.revs_info = d
            return self.revs_info
        else :
            if self.doc :
                s, h, d = _headdoc( conn, paths, hthdrs=hthdrs )
                if s == OK and h['Etag'] == self.doc['_rev'] :
                    return self
            s, h, d = _readdoc( conn, paths, hthdrs=hthdrs )
            self.doc = d
            return self

    def items( self ) :
        """Dictionary method to provide a list of (key,value) tuple"""
        return self.doc.items()

    def all( self ) :
        """Shortcut for,
            doc( revs=True )

        Returns,
            JSON converted object containing a list of revisions for the
            document.
        Admin-prev,
            No
        """
        return self( revs=True )

    def update( self, using={}, hthdrs={} ) :
        """Update JSON converted document object with a dictionary.
        Updating the document using this interface will automatically PUT the
        document into the database. However, the following keys cannot be
        updated,
            `_id`, `_rev`
        Optionally accepts HTTP headers `hthdrs`. Calling it with empty
        argument will simply put the existing document content into the
        database.
        To refresh the Document object, so that it reflects the database
        document, call the Document object, `doc()`

        Return,
            None
        Admin-prev,
            No
        """
        [ using.pop( k, None ) for k in ['_id', '_rev'] ]
        self.doc.update( using )
        conn, paths = self.conn, self.paths
        s, h, d = _updatedoc( conn, self.doc, paths, hthdrs=hthdrs, **query )
        self.doc.update({ '_rev' : d['rev'] }) if d else None
        return None

    def delitem( self, key ) :
        """Remove the specified key from document. However, the following keys
        cannot be removed,
            `_id`, `_rev`
        To refresh the Document object, so that it reflects the database
        document, call the Document object, `doc()`
        
        Return,
            Value of the deleted key
        Admin-prev,
            No
        """
        reserved = [ '_id', '_rev' ]
        if key in reserved : return None
        val, _ = ( self.doc.pop(key, None), self.update() )
        self.revs = None            # Enforce a fetch
        self.revs_info = None       # Enforce a fetch
        return val

    def copyto( self, toid, asrev=None ) :
        """Copy this revision of document to a destination specified by
        `toid` and optional revision `asrev`. The source document revision
        will be same as this Document object.

        Return,
            On success, copied document object, else None
        Admin-prev,
            No
        """
        return self.__class__.copy( self.db, self, toid, asrev=asrev,
                                    rev=self._rev )

    def addattach( self, filepath, content_type=None, hthdrs={}, **query ) :
        """Add file `filepath` as attachment to this document. HTTP headers
        'Content-Type' and 'Content-Length' will also be remembered in the
        database. Optinally, content_type can be provided as key-word
        argument.
        
        Return,
            Attachment object
        Admin-prev,
            No
        """
        data = open( filepath ).read()
        filename = basename( filepath )
        d = Attachment.putattachment(
                self.db, self, filename, data, content_type=content_type,
                hthdrs=hthdrs, **query
            )
        self.doc.update({ '_rev' : d['rev'] }) if d != None else None
        return Attachment( self, filename )

    def delattach( self, attach, hthdrs={}, **query ) :
        """Delete the attachment either specified as filename or Attachment
        object from this document
        """
        filename = attach.filename \
                   if isinstance(attach, Attachment) else attach
        d = Attachment.delattachment(
                self.db, self, filename, hthdrs=hthdrs, **query
            )
        self.doc.update({ '_rev' : d['rev'] }) if d != None else None
        return None

    def attach( self, filename ) :
        """Return Attachment object for filename in this document"""
        a_ = self.doc.get( '_attachment', {} ).get( filename, None )
        a = Attachment( self, filename ) if a_ else None
        return a

    def attachs( self ) :
        """Return a list of all Attachment object for attachment in this
        document
        """
        a = [ Attachment(self, f) for f in self.doc.get('_attachments', {}) ]
        return a

    _id = property( lambda self : self.doc['_id'] )
    _rev = property( lambda self : self.doc['_rev'] )
    _attachments = property( lambda self : self.doc.get('_attachments', {}) )
    _deleted = property( lambda self : self.doc.get('_deleted', {}) )
    _conflict = property( lambda self : self.doc.get('_conflict', {}) )

    @classmethod
    def head( cls, db, doc, hthdrs={}, **query ) :
        id_ = doc if isinstance(doc, basestring) else doc['_id']
        paths = db.paths + [ id_ ]
        s, h, d = _headdoc( db.conn, paths, hthdrs=hthdrs, **query )
        return s, h, d

    @classmethod
    def create( cls, db, doc, attachfiles=[], hthdrs={}, **query ) :
        """Create a new document in the specified database, using the supplied
        JSON document structure. If the JSON structure includes the _id
        field, then the document will be created with the specified document
        ID. If the _id field is not specified, a new unique ID will be
        generated.

        query parameters,
        batch,
            if specified 'ok', allow document store request to be batched
            with others

        Return,
            Document object
        Admin-prev,
            No
        """
        id_ = [ doc['_id'] ] if '_id' in doc else []
        if not self.validate_docid(id_) : 
            return None
        paths = db.paths + id_
        attachs = Attachment.files2attach(attachfiles)
        doc.update( '_attachments', attachs ) if attachs else None
        s, h, d = _createdoc( db.conn, doc, paths, hthdrs, **query )
        if d == None : return None
        doc.update({ '_id' : d['id'], '_rev' : d['rev'] })
        return Document( db, doc, fetch=False )

    @classmethod
    def delete( cls, db, doc, hthdrs={}, **query ) :
        """Delete a document in the specified database. `doc` can be
        document-id or it can be Document object, in which case the object is
        not valid after deletion.

        query parameters,
        rev,
            the current revision of the document.

        Return,
            JSON converted object as returned by CouchDB
        Admin-prev,
            No
        """
        id_ = doc if isinstance(doc, basestring) else doc['_id']
        paths = db.paths + id_
        s, h, d = _deletedoc( db.conn, doc, paths, hthdrs, **query )
        return d

    @classmethod
    def copy( cls, db, doc, toid, asrev=None, hthdrs={}, **query ) :
        """Copy a source document to a destination, specified by
        `toid` and optional revision `asrev`. If the source document's
        revision `rev` is not provided as key-word argument, then the latest
        revision of the document will be used.

        Return,
            On success, destination's Document object, else None
        Admin-prev,
            No
        """
        id_ = doc if isinstance(doc, basestring) else doc['_id']
        dest = toid if asrev == None else "%s?rev=%s" % (toid, asrev),
        hthdrs = { 'Destination' : dest }
        paths = db.paths + [ id_ ]
        s, h, d = _copydoc( db.conn, paths, hthdrs=hthdrs, **query )
        if 'id' in d and 'rev' in d :
            return Document( self.db, d['id'], hthdrs=hthdrs, rev=d['rev'] )
        else :
            return None

    SPECIAL_DOC_NAMES = set([
        '_all_docs',
        '_design',
        '_changes', '_compact',
        '_view_cleanup', '_temp_view',
        '_ensure_full_commit', '_bulk_docs', '_purge',
        '_missing_revs', '_revs_diff', '_revs_limit', 
        '_security',
        '_local',
    ])
    def validate_docid( self, docid ) :
        return not (docid in SPECIAL_DOC_NAMES)



class LocalDocument( Document ) :

    def __init__( self, db, doc, hthdrs={}, **query ) :
        """
        Read the local document specified by doc, which can be either
        a dictionary
        containing `_id` key or a string to be interpreted as `_id` from
        database `db`. Optionally accepts HTTP headers `hthdrs`.

        query parameters,

        rev,
            Specify the revision to return
        revs,
            Return a list of the revisions for the document
        revs_info,
            Return a list of detailed revision information for the document

        Return,
            Document object
        Admin-prev,
            No
        """
        Document.__init__( self, db, doc, hthdrs={}, **query )
        id_ = doc if isinstance(doc, basestring) else doc['_id']
        id_ = self.id2name( id_ )
        self.paths = db.paths + [ '_local',  id_ ]

    @classmethod
    def create( cls, db, doc, hthdrs={}, **query ) :
        """Create a new local document in the specified database, using the
        supplied JSON document structure. Unlike for Document objects, the
        JSON structure must include _id field.

        query parameters,
        batch,
            if specified 'ok', allow document store request to be batched
            with others

        Return,
            LocalDocument object
        Admin-prev,
            No
        """
        id_ = doc['_id']
        if not self.validate_docid(id_) : 
            return None
        paths = db.paths + [ '_local', id_ ]
        s, h, d = _updatedoc( db.conn, doc, paths, hthdrs, **query )
        if d == None : return None
        doc.update({ '_rev' : d['rev'] })
        return LocalDocument( db, doc, fetch=False )

    @classmethod
    def delete( cls, db, doc, hthdrs={}, **query ) :
        """Delete a document in the specified database. `doc` can be
        document-id or it can be Document object, in which case the object is
        not valid after deletion.

        query parameters,
        rev,
            the current revision of the document.

        Return,
            JSON converted object as returned by CouchDB
        Admin-prev,
            No
        """
        id_ = doc if isinstance(doc, basestring) else doc['_id']
        paths = db.paths + [ '_local', id_ ]
        s, h, d = _deletedoc( db.conn, doc, paths, hthdrs, **query )
        return d

    @classmethod
    def copy( cls, db, doc, toid, asrev=None, hthdrs={}, **query ) :
        """Copy a source document to a destination, specified by
        `toid` and optional revision `asrev`. If the source document's
        revision `rev` is not provided as key-word argument, then the latest
        revision of the document will be used.

        Return,
            On success, destination's Document object, else None
        Admin-prev,
            No
        """
        id_ = doc if isinstance(doc, basestring) else doc['_id']
        dest = toid if asrev == None else "%s?rev=%s" % (toid, asrev),
        hthdrs = { 'Destination' : dest }
        paths = db.paths + [ '_local', id_ ]
        s, h, d = _copydoc( db.conn, paths, hthdrs=hthdrs, **query )
        if 'id' in d and 'rev' in d :
            return LocalDocument( self.db, d['id'], hthdrs=hthdrs, rev=d['rev'] )
        else :
            return None

    def id2name( self, id_ ) :
        return id_[6:] if id_.startswith( '_local' ) else id_
