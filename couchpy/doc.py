import sys, re, json, time
from   os.path          import basename
from   copy             import deepcopy
from   StringIO         import StringIO
from   mimetypes        import guess_type
import base64

from   httperror        import *
from   httpc            import HttpSession, ResourceNotFound, OK, CREATED
from   database         import Database
from   CouchPy          import CouchPyError

# TODO :
#   1. creating document without id shouldbe implemented as Database.<method>
#   2. Batch mode POST / PUT should have a verification system built into it.
#   3. Attachments allowed in local documents ???

log = configlog( __name__ )

""" Document Structure
{
    '_id' : <unique id>
    '_rev' : <current-revno>
    '_attachments' : {
        <filename> : {
            'content_type' : '<content-type>'
            'data' : '<base64-encoded data>'
        },
        <filename> : {
            'content_type' : '<content-type>'
            'length' : '<len>'
            'revpos' : '<doc-revision>'
            'stub'   : '<bool>'
        },
        ....
    '_deleted' : true,
    }
"""

hdr_acceptjs = { 'Accept' : 'application/json' }

def _createdoc( conn, doc, paths=[], hthdrs={}, **query ) :
    # query : batch='ok'
    body = rest.data2json( doc )
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.post( paths, hthdrs, body, _query=query.items() )

    if s == OK and d['ok'] == True :
        return s, h, d
    else :
        return (None, None, None)

def _updatedoc( conn, doc, paths=[], hthdrs={}, **query ) :
    # query : batch='ok'
    if '_id' not in doc :
        raise CouchPyError( '`_id` to be supplied while updating the doc' )
    if '_rev' not in doc :
        raise CouchPyError( '`_rev` to be supplied while updating the doc' )
    body = rest.data2json( data )
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.put( paths, hthdrs, body, _query=query.items() )

    if s == OK and d['ok'] == True :
        return s, h, d
    else :
        return (None, None, None)

def _deletedoc( conn, doc, paths=[], hthdrs={}, **query ) :
    # query : rev=<_rev>
    if 'rev' not in query :
        raise CouchPyError( '`rev` to be supplied while deleteing the doc' )
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.head( paths, hthdrs, None, _query=query.items() )
    if s == OK and d['ok'] == True :
        return s, h, d
    else :
        return (None, None, None)

def _readdoc( conn, paths=[], hthdrs={}, **query ) :
    # query : rev=<_rev>, revs=<'true'>, revs_info=<'true'>
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.get( paths, hthdrs, None, _query=query.items() )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _headdoc( conn, paths=[], hthdrs={}, **query ) :
    # query : anything
    s, h, d = conn.head( paths, hthdrs, None, _query=query.items() )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _copydoc( conn, paths=[], hthdrs={}, **query ) :
    # query : rev=<_srcrev>
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
        # TODO : 
        #   1. url-encode _id ???
        self.db = db
        self.client = db.client
        self.conn = db.conn
        self.debug = db.debug
        id_ = doc if isinstance(doc, basestring) else doc['_id']
        self.paths = db.paths + [ id_ ]
        self.revs = None        # Cached object
        self.revs_info = None   # Cached object
        s, h, d = _readdoc( self.conn, self.paths, hthdrs=hthdrs, **query
                  ) if fetch == True else (None, None, {})
        self.doc = d

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
        self.doc.update({ key, value })
        s, h, d = _updatedoc( self.conn, self.doc, self.paths )
        self.doc.update({ '_rev' : d['rev'] }) if d else None
        self.revs = None            # Enforce a fetch
        self.revs_info = None       # Enforce a fetch
        return None

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
            return Document( self.db, self.doc, hthdrs=hthdrs, rev=rev )
        elif revs == True :
            q = { 'revs' : 'true' }
            if self.revs :
                s, h, d = _headdoc( conn, paths, hthdrs=hthdrs, **q )
                if s == OK and h['Etag'] == self.doc['_rev'] :
                    return self.revs
            s, h, d = _readdoc( conn, paths, hthdrs=hthdrs, **q )
            return d
        elif revs_info == True :
            q = { 'revs_info' : 'true' }
            if self.revs_info
                s, h, d = _headdoc( conn, paths, hthdrs=hthdrs, **q )
                if s == OK and h['Etag'] == self.doc['_rev'] :
                    return self.revs_info

            s, h, d = _readdoc( conn, paths, hthdrs=hthdrs, **q )
            return d
        else :
            if self.doc :
                s, h, d = _headdoc( conn, paths, hthdrs=hthdrs )
                if s == OK and h['Etag'] == self.doc['_rev'] :
                    return self
            s, h, d = _readdoc( conn, paths, hthdrs=hthdrs )
            self.doc = d
            return self

    def __iter__(self):
        """Yield a key,value pair for every iteration"""
        return iter( self.doc.items() )

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

    def update(self, using={} hthdrs={} ) :
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
        using.pop( '_id', None )
        using.pop( '_rev', None )
        self.doc.update( using )
        conn, paths = self.conn, self.paths
        s, h, d = _updatedoc( conn, self.doc, paths, hthdrs=hthdrs, **query )
        self.doc.update({ '_rev' : d['rev'] }) if d else None
        return None

    def items( self ) :
        """Dictionary method to provide a list of (key,value) tuple"""
        return self.doc.items()

    def delitem( self, key ) :
        """Delete a particular key, value pair from this document

        Return,
            Value of the deleted key
        Admin-prev,
            No
        """
        val, _ = ( self.doc.pop(key), self.update()
                 ) if key in self.doc else ( None, None )
        conn, paths = self.conn, self.paths
        s, h, d = _updatedoc( conn, self.doc, paths, hthdrs=hthdrs, **query )
        self.doc.update({ '_rev' : d['rev'] }) if d else None
        return val

    def copy( self, toid, asrev=None ) :
        """Copy this revision of document to a destination specified by
        `toid` and optional revision `asrev`. The source document revision
        will be same as this Document object.

        Return,
            On success, copied document object, else None
        Admin-prev,
            No
        """
        return Document.copy( self.db, self, toid, asrev=asrev, rev=self._rev )

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

    @classmethod
    def head( db, doc, hthdrs={}, **query ) :
        id_ = doc if isinstance(doc, basestring) else doc['_id']
        paths = db.paths + [ id_ ]
        s, h, d = _headdoc( db.conn, paths, hthdrs=hthdrs, **query )
        return s, h, d

    @classmethod
    def create( db, doc, attachfiles=[], hthdrs={}, **query ) :
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
        id_ = [ doc.pop( '_id' ) ] if '_id' in doc else []
        if not self.validate_docid(id_) : 
            return None
        paths = db.paths + id_
        attachs = Attachment.file2attach(attachfiles)
        doc.update( '_attachments', attachs ) if attachs else None
        s, h, d = _createdoc( db.conn, doc, paths, hthdrs, **query )
        if d == None : return None
        doc.update({ '_id' : d['id'], '_rev' : d['rev'] })
        return Document( db, doc, fetch=False )

    @classmethod
    def delete( db, doc, hthdrs={}, **query ) :
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
        id_ = doc if isinstance(doc, basestring) else doc.pop('_id')
        paths = db.paths + id_
        s, h, d = _deletedoc( db.conn, doc, paths, hthdrs, **query )
        return d

    @classmethod
    def copy( db, doc, toid, asrev=None, hthdrs={}, **query ) :
        """Copy a source document to a destination, specified by
        `toid` and optional revision `asrev`. If the source document's
        revision `rev` is not provided as key-word argument, then the latest
        revision of the document will be used.

        Return,
            On success, destination's Document object, else None
        Admin-prev,
            No
        """
        id_ = doc if isinstance(doc, basestring) else doc.pop('_id')
        dest = toid if asrev == None else "%s?rev=%s" % (toid, asrev),
        hthdrs = { 'Destination' : dest }
        paths = db.paths + id_
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

def _readattach( conn, paths=[], hthdrs={}, **query ) :
    s, h, d = conn.get( paths, hthdrs, None, _query=query.items() )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _writeattach( conn, paths=[], body, hthdrs={}, **query ) :
    # query rev=<_rev>
    s, h, d = conn.put( paths, hthdrs, body, _query=query.items() )
    if s == OK and d['ok'] == True :
        return s, h, d
    else :
        return (None, None, None)

def _deleteattach( conn, paths=[], hthdrs={}, **query ) :
    # query rev=<_rev>
    s, h, d = conn.delete( paths, hthdrs, _query=query.items() )
    if s == OK and d['ok'] == True :
        return s, h, d
    else :
        return (None, None, None)

class Attachment( object ) :

    def __init__( self, doc, filename ) :
        """Instance object representing a single attachment in a document, use
        the Document object and attachment `filename` to create the instance.
        """
        self.doc = doc
        self.filename = filename

    def attachinfo( self, field=None ) :
        """Information from attachment stub in the document. If `field`
        key-word argument is provided, value of that particular field is
        returned, otherwise, entire dictionary of information is returned
        """
        a = self.doc.doc.get( '_attachment', {} ).get( filename, None )
        return a if field == None else a.get( field, None )

    def data( self, hthdrs={} ) :
        """
        Returns the content of the file attached to the document. Can
        optionally take a dictionary of http headers.
        """
        s, h, d = self.getattachment( 
                        self.db, self.doc, self.filename, hthdrs=hthdrs )
        return d

    content_type = property( lambda self : self.attachinfo('content_type') )
    length = property( lambda self : self.attachinfo('length') )
    revpos = property( lambda self : self.attachinfo('revpos') )
    stub = property( lambda self : self.attachinfo('stub') )
    content = property(data)
        
    @classmethod
    def getattachment( db, doc, filename, hthdrs={} ) :
        """Returns a tuple of,
            ( <filedata>, <content_type> )
        for attachment `filename` in `doc` stored in database `db`
        """
        id_ = doc if isinstance(doc, basestring) else doc._id
        paths = db.paths + [ id_, filename ]
        s, h, d = _readattach( db.conn, paths, hthdrs=hthdrs )
        content_type = h.get( 'Content-Type', None )
        return (d, content_type)

    @classmethod
    def putattachment( db, doc, filename, data, content_type=None, hthdrs={},
                       **query ) :
        """Upload the supplied content (data) as attachment to the specified
        document (doc). `filename` provided must be a URL encoded string.
        If `doc` is document-id, then `rev` keyword parameter should be
        present in query.
        """
        id_ = doc if isinstance(doc, basestring) else doc._id
        rev = query['rev'] if 'rev' in query else doc['_rev']
        paths = db.paths + [ id_, filename ]
        hthdrs = deepcopy( hthdrs )
        hthdrs.update(
            {'Content-Type' : content_type} if content_type != None else {}
        )
        s, h, d = _writeattach( db.conn, paths, data, hthdrs=hthdrs, rev=rev )
        if isinstance(doc, Document) and d != None :
            doc.update({ '_rev' : d['rev'] })
        return d

    @classmethod
    def delattachment( db, doc, filename, hthdrs={}, **query ) :
        """Deletes the attachment form the specified doc. You must
        supply the rev argument with the current revision to delete the
        attachment."""
        id_ = doc if isinstance(doc, basestring) else doc._id
        rev = query['rev'] if 'rev' in query else doc['_rev']
        paths = db.paths + [ id_, filename ]
        s, h, d = _deleteattach( db.conn, paths, data, hthdrs=hthdrs, rev=rev )
        if isinstance(doc, Document) and d != None :
            doc.update({ '_rev' : d['rev'] })
        return d

    @classmethod
    def file2attach( fnames=[] ) :
        """Helper method that will convert specified files `fnames` into
        attachment structures in document format (key, value) pairs that is
        suitable for writing into CouchDB.
        """
        fnames = ( isinstance(fnames, basestring) and [fnames] ) or fnames
        attachs = {}
        for f in fnames :
            if isinstance(f, (list,tuple)) :
                ctype, fname = f
                fdata = base64.encodestring( open(fname).read() )
                attachs.setdefault(
                        fname, { 'content_type' : ctype, 'data' : data }
                )
            elif isinstance(f, basestring) :
                ctype = guess_type(f)
                fname, data = f, base64.encodestring( open(f).read() )
                attachs.setdefault(
                        fname, { 'content_type' : ctype, 'data' : data }
                )
        return attachs


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
        self.paths = db.paths + [ '_local',  id_ ]

    def copy( self, *args, **kwargs ) :
        """This method is not supported for loca-documents"""
        return None

""" Design document structure,
{
  '_id'      : '_design/<design-docname>',
  '_rev'     : '<rev-md5>',             /* Will be generated by the server */
  'language' : '<viewserver-language>',
  'views'    : {
    '<viewname>' : {
       'map'    : function( doc ) { ... }
       'reduce' : function( keys, values, rereduce ) { ... }
    },
  },
  '_deleted' : true,
}
"""
