from   copy               import deepcopy

from   httperror          import *
from   httpc              import HttpSession, ResourceNotFound, OK, CREATED
from   couchpy            import CouchPyError
from   couchpy.attachment import Attachment

# TODO :
#   1. List, Update and Rewirte APIs are still being defined.
#   2. HEAD method is not supported for design-documents ??? If available, can
#   be used for caching

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
  '_conflict' : true,
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
}
"""

hdr_acceptjs = { 'Accept' : 'application/json' }
hdr_ctypejs  = { 'Content-Type' : 'application/json' }

def _readsgn( conn, paths=[], hthdrs={}, **query ) :
    """GET /<db>/_design/<doc>
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

def _updatesgn( conn, doc, paths=[], hthdrs={} ) :
    """PUT /<db>/_design/<doc>
    """
    if '_id' not in doc :
        err = '`_id` to be supplied while updating the design doc'
        raise CouchPyError( err )
    body = rest.data2json( doc )
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    hthdrs.update( hdr_ctypejs )
    s, h, d = conn.put( paths, hthdrs, body )
    if s == OK and d['ok'] == True :
        return s, h, d
    else :
        return (None, None, None)


def _deletesgn( conn, paths=[], hthdrs={}, **query ) :
    """DELETE /<db>/_design/<doc>
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

def _copysgn( conn, paths=[], hthdrs={}, **query ) :
    """COPY /<db>/_design/<doc>
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

def _infosgn( conn, paths=[], hthdrs={} ) :
    """GET /<db>/_design/<design-doc>/_info"""
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    s, h, d = conn.delete( paths, hthdrs, None )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _showsgn( conn, paths=[], doc=None, hthdrs={}, **query ) :
    """
    GET  /<db>/_design/<design-doc>/_show/<show-name>,
         if doc is none
    POST /<db>/_design/<design-doc>/_show/<show-name>/<doc>,
         if doc is design-document id
    query for GET,
        details=<string>   format=<string>
    """
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    hthdrs.update( hdr_ctypejs )
    if doc == None :
        s, h, d = conn.get( paths, hthdrs, None, _query=query.items() )
    else :
        paths = paths + [ doc ]
        s, h, d = conn.get( paths, hthdrs, None, _query=query.items() )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)


class DesignDocument( object ) :

    def __init__( self, db, doc, fetch=True, hthdrs={}, **query ) :
        """Read the design-document specified by `doc` which can be either a
        dictionary containing `_id` key or a string to be interpreted as
        design-document `_id`. If key-word argument `fetch` is passed as False,
        then the document will not be fetched from the database. Optionally
        accepts HTTP headers `hthdrs`.

        query parameters,

        rev,
            Specify the revision to return
        revs,
            Return a list of the revisions for design-document
        revs_info,
            Return a list of detailed revision information for the
            design-document

        Return,
            DesignDocument object
        Admin-prev,
            No
        """
        # TODO : url-encode _id ???
        self.db = db
        self.conn = db.conn

        id_ = doc if isinstance(doc, basestring) else doc['_id']
        id_ = self.id2name( id_ )
        self.paths = db.paths + [ '_design', id_ ]
        s, h, d = _readsgn( self.conn, self.paths, hthdrs=hthdrs, **query
                  ) if fetch == True else (None, None, {})
        self.doc = d

        self.revs = None        # Cached object
        self.revs_info = None   # Cached object
        self.client = db.client
        self.debug = db.debug

    def __getitem__(self, key) :
        """Read JSON converted design-document object like a dictionary"""
        return self.doc.get( key, None )

    def __setitem__(self, key, value) :
        """Update JSON converted design-document object like a dictionary.
        Updating the document object using this interface will automatically
        PUT the document into the database. However, the following keys cannot
        be updated,
            `_id`, `_rev`
        To refresh the DesignDocument object, so that it reflects the
        database document, call the DesignDocument object, `doc()`
        
        Return,
            None
        Admin-prev,
            No
        """
        reserved = [ '_id', '_rev' ]
        if key in reserved : return None
        self.doc.update({ key : value })
        s, h, d = _updatesgn( self.conn, self.doc, self.paths )
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
        If no argument is speficied refresh the design-document from database.
        Optionally accepts HTTP headers `hthdrs`.

        query parameters,
        rev,
            If specified, and not the same as this DesignDocument
            object's revision, create a fresh DesignDocument object with the
            document of specified revision read from database.
        revs,
            If True, return JSON converted object containing a list of
            revisions for the design-document. Structure of the returned object is
            defined by CouchDB
        revs_info,
            If True, return JSON converted object containing extended
            information about all revisions of the design-document. Structure
            of the returned object is defined by CouchDB.
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
            s, h, d = _readsgn( conn, paths, hthdrs=hthdrs, **q )
            self.revs = d
            return self.revs
        elif revs_info == True :
            q = { 'revs_info' : 'true' }
            s, h, d = _readsgn( conn, paths, hthdrs=hthdrs, **q )
            self.revs_info = d
            return self.revs_info
        else :
            s, h, d = _readsgn( conn, paths, hthdrs=hthdrs )
            self.doc = d
            return self

    def items( self ) :
        """Dictionary method to provide a list of (key,value) tuple"""
        return self.doc.items()

    def all( self ) :
        """Shortcut for,
            designdoc( revs=True )

        Returns,
            JSON converted object containing a list of revisions for the
            design-document.
        Admin-prev,
            No
        """
        return self( revs=True )

    def update( self, using={}, hthdrs={} ) :
        """Update JSON converted design-document object with a dictionary.
        Updating the document using this interface will automatically PUT the
        document into the database. However, the following keys cannot be
        updated,
            `_id`, `_rev`
        Optionally accepts HTTP headers `hthdrs`. Calling it with empty
        argument will simply put the existing document content into the
        database.
        To refresh the DesignDocument object, so that it reflects the database
        document, call the DesignDocument object, `doc()`

        Return,
            None
        Admin-prev,
            No
        """
        [ using.pop( k, None ) for k in ['_id', '_rev'] ]
        self.doc.update( using )
        conn, paths = self.conn, self.paths
        s, h, d = _updatesgn( conn, self.doc, paths, hthdrs=hthdrs, **query )
        self.doc.update({ '_rev' : d['rev'] }) if d else None
        return None

    def delitem( self, key ) :
        """Remove the specified key from design-document. However, the
        following keys cannot be removed,
            `_id`, `_rev`
        To refresh the DesignDocument object, so that it reflects the database
        document, call the DesignDocument object, `doc()`
        
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
        """Copy this revision of design-document to a destination specified by
        `toid` and optional revision `asrev`. The source document revision
        will be same as this DesignDocument object.

        Return,
            On success, copied DesignDocument object, else None
        Admin-prev,
            No
        """
        return self.__class__.copy( self.db, self, toid, asrev=asrev,
                                    rev=self._rev )

    def addattach( self, filepath, content_type=None, hthdrs={}, **query ) :
        """Add file `filepath` as attachment to this design-document.
        HTTP headers 'Content-Type' and 'Content-Length' will also be
        remembered in the database. Optinally, content_type can be provided
        as key-word argument.
        
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
        object from this design-document
        """
        filename = attach.filename \
                   if isinstance(attach, Attachment) else attach
        d = Attachment.delattachment(
                self.db, self, filename, hthdrs=hthdrs, **query
            )
        self.doc.update({ '_rev' : d['rev'] }) if d != None else None
        return None

    def attach( self, filename ) :
        """Return Attachment object for filename in this design-document"""
        a_ = self.doc.get( '_attachment', {} ).get( filename, None )
        a = Attachment( self, filename ) if a_ else None
        return a

    def attachs( self ) :
        """Return a list of all Attachment object for attachment in this
        design-document
        """
        a = [ Attachment(self, f) for f in self.doc.get('_attachments', {}) ]
        return a

    def info( self, hthdrs={} ) :
        """Obtains information about a given design document, including the
        index, index size and current status of the design document and
        associated index information.

        Returns,
            JSON converted object as returned by CouchdB.
        Admin-prev,
            No
        """
        conn, paths = self.conn, self.paths
        s, h, d = _infosgn( conn, paths, hthdrs=hthdrs )
        return d

    def views( self ) :
        """Return a list of view names from this design document.

        Return,
            List of view names
        """
        self()
        return self.views.keys()

    _id = property( lambda self : self.doc['_id'] )
    _rev = property( lambda self : self.doc['_rev'] )
    _attachments = property( lambda self : self.doc.get('_attachments', {}) )
    _deleted = property( lambda self : self.doc.get('_deleted', {}) )
    _conflict = property( lambda self : self.doc.get('_conflict', {}) )
    language = property( lambda self : self.doc.get('language', {}) )
    views = property( lambda self : self.doc.get('views', {}) )

    @classmethod
    def create( cls, db, doc, hthdrs={} ) :
        """Create a new design-document in the specified database, using the
        supplied JSON document structure. If the JSON structure includes the _id
        field, then the document will be created with the specified document
        ID. If the _id field is not specified, a new unique ID will be
        generated.

        Return,
            DesignDocument object
        Admin-prev,
            No
        """
        id_ = self.id2name( doc['_id'] )
        if not cls.validate_docid(id_) : 
            return None
        paths = db.paths + [ '_design', id_ ]
        s, h, d = _updatesgn( db.conn, doc, paths, hthdrs )
        if d == None : return None
        doc.update({ '_id' : d['id'], '_rev' : d['rev'] })
        return DesignDocument( db, doc, fetch=False )

    @classmethod
    def delete( cls, db, doc, hthdrs={}, **query ) :
        """Delete a design-document in the specified database. `doc` can be
        document-id or it can be DesignDocument object, in which case the object
        is not valid after deletion.

        query parameters,
        rev,
            the current revision of the document.

        Return,
            JSON converted object as returned by CouchDB
        Admin-prev,
            No
        """
        id_ = self.id2name(doc if isinstance(doc, basestring) else doc['_id'])
        paths = db.paths + [ '_design', id_ ]
        s, h, d = _deletesgn( db.conn, paths, hthdrs, **query )
        return d

    @classmethod
    def copy( cls, db, doc, toid, asrev=None, hthdrs={}, **query ) :
        """Copy a source design-document to a destination, specified by
        `toid` and optional revision `asrev`. If the source document's
        revision `rev` is not provided as key-word argument, then the latest
        revision of the document will be used.

        Return,
            On success, destination's DesignDocument object, else None
        Admin-prev,
            No
        """
        id_ = self.id2name(doc if isinstance(doc, basestring) else doc['_id'])
        paths = db.paths + [ '_design', id_ ]
        dest = toid if asrev == None else "%s?rev=%s" % (toid, asrev),
        hthdrs = { 'Destination' : dest }
        s, h, d = _copysgn( db.conn, paths, hthdrs=hthdrs, **query )
        if 'id' in d and 'rev' in d :
            return DesignDocument( self.db, d['id'], hthdrs=hthdrs, rev=d['rev'] )
        else :
            return None

    SPECIAL_DOC_NAMES = [
    ]
    @classmethod
    def validate_docid( cls, docid ) :
        return not (docid in cls.SPECIAL_DOC_NAMES)

    def id2name( self, id_ ) :
        return id_[7:] if id_.startswith( '_design' ) else id_
