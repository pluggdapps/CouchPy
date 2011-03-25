"""CouchDB is a document database and every document is stored in JSON format.
Fortunately, JSON formated objects can easily be converted to native python
objects. Document class defines a collection of attributes and methods to
access CouchDB documents.

>>> c = Client()
>>> db = c.create( 'testdb' )

Create a document by name 'Fishstew', with a list of `files` attached to it.

>>> doc = { _id='Fishstew' }
>>> doc = Document.create( db, doc, attachfiles=files )
>>> doc1 = { _id='ChickenTikaa' }
>>> doc1 = Document.create( db, 'ChickenTikaa', attachfiles=files, batch='ok' )

Fetch document,

>>> doc = Document( db, 'Fishstew' )               # Fetch latest revision
>>> doc = Document( db, 'Fishstew', rev=u'1-1eb6f37b091a143c69ed0332de74df0b' # Fetch a particular revision
>>> revs = Document( db, doc, revs=True )          # Fetch revision list
>>> revsinfo = Document( db, doc, revs_info=True ) # Fetch extended revisions

Access document object,

>>> doc['tag'] = 'seafood'      # Create / update a new field 
>>> doc['tag']                  # Access key, value pair
seafood
>>> doc._id                     # Document ID
Fishstew
>>> doc._rev                    # Document Revision
u'1-1eb6f37b091a143c69ed0332de74df0b'
>>> doc.items()                 # List of (key,value) pairs
[(u'_rev', u'1-1eb6f37b091a143c69ed0332de74df0b'), (u'_id', u'Fishstew'), (u'tag', u'seafood')]
>>> doc.update({ 'key1' : 'value1' })
>>> [ (k,v) for k,v in doc ]    # Same as doc.items()
>>> doc.delitem( 'key1' )       # Delete key,value pair
>>> del doc['key1']             # Same as doc.delitem

Manage document attachments,

>>> a = doc.addattach( '/home/user/recipe.txt' )  # Attach file to document
>>> doc.delattach( a )                            # Delete attachment
>>> doc.attachs()                                 # Get a list of Attachment objects
>>> a = doc.attach( 'recipe.txt' )
>>> a.filename                      # Attachment filename 
receipe.txt
>>> a.data()
( ... file content ..., text/plain )

Delete document,

>>> Document.delete( db, doc1 )

Copy document,

>>> bkpdoc = Document.copy( db, doc._id, 'Fishstew-bkp', rev=doc._rev )

"""

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
import rest

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
hdr_ctypejs  = { 'Content-Type' : 'application/json' }

def _createdoc( conn, doc, paths=[], hthdrs={}, **query ) :
    """POST /<db>/<doc>
    query,
        batch='ok'
    """
    body = rest.data2json( doc )
    hthdrs = deepcopy( hthdrs )
    hthdrs.update( hdr_acceptjs )
    hthdrs.update( hdr_ctypejs )
    s, h, d = conn.post( paths, hthdrs, body, _query=query.items() )
    if s == CREATED and d['ok'] == True :
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
    hthdrs.update( hdr_ctypejs )
    s, h, d = conn.put( paths, hthdrs, body, _query=query.items() )
    if s == CREATED and d['ok'] == True :
        return s, h, d
    else :
        return (None, None, None)

def _deletedoc( conn, paths=[], hthdrs={}, **query ) :
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
    if s == CREATED :
        return s, h, d
    else :
        return (None, None, None)


class Document( object ) :

    def __init__( self, db, doc, fetch=True, hthdrs={}, **query ) :
        """Instantiate python representation of a CouchDB document, specified
        by ``doc`` for database ``db``. ``db`` should be a
        :class:`couchpy.database.Database` object, while ``doc``
        can either be a dictionary of key, value pairs (where one of the key
        should be ``_id``) convertible to JSON or a string to be interpreted
        as document ``_id``. By default, the document identified by ``_id``
        will be fetched from the database, converted and encapsulated into
        this object. To avoid database access, pass key-word argument
        ``fetch`` as False. Optionally accepts HTTP headers ``hthdrs``, which
        will be remembered for all database access initiated via this object.

        Optional arguments:

        ``rev``,
            Specify document's revision to fetch.

        Admin-prev, No
        """
        # TODO : url-encode _id ???
        self.db = db
        self.conn = db.conn

        id_ = doc if isinstance(doc, basestring) else doc['_id']
        self.paths = db.paths + [ id_ ]
        s, h, doc = _readdoc( self.conn, self.paths, hthdrs=hthdrs, **query
                    ) if fetch == True else (None, None, doc)
        self.doc = doc

        self.revs = None        # Cached object
        self.revs_info = None   # Cached object
        self.client = db.client
        self.debug = db.debug
        self.hthdrs = hthdrs

    def __getitem__(self, key) :
        """Fetch a value corresponding to the ``key`` stored in this document.
        This method does not trigger a database access, instead it uses the
        cached representation of the document. To refresh the document from
        database, do, `doc()`
        """
        return self.doc.get( key, None )

    def __setitem__(self, key, value) :
        """Update the ``key`` value with ``value``. Updating the document
        object using this interface will automatically PUT the document into
        the database. However, the following keys cannot be updated, ``_id``,
        ``_rev``. To refresh the document from database, do, `doc()`
        
        Admin-prev, No
        """
        reserved = [ '_id', '_rev' ]
        if key in reserved : return None
        self.doc.update({ key : value })
        h = deepcopy( self.hthdrs )
        s, h, d = _updatedoc( self.conn, self.doc, self.paths, hthdrs=h )
        self.doc.update({ '_rev' : d['rev'] }) if d else None
        self.revs = None            # Enforce a fetch
        self.revs_info = None       # Enforce a fetch
        return None

    def __delitem__( self, key ) :
        """Delete key, value pair identified by ``key``. Python shortcut for
        :func:`Document.delitem`"""
        return self.delitem( key )

    def __iter__(self):
        """Yield document's key,value pair for every iteration"""
        return iter( self.doc.items() )

    def __call__( self, hthdrs={}, **query ) :
        """If no argument is speficied, refresh the document from database.
        Optionally accepts HTTP headers `hthdrs`.

        Optional arguments:

        ``rev``,
            If specified, and not the same as this Document object's
            revision, create a fresh :class:`Document` object corresponding to
            the specified revision. Fetch the same from database.
        ``revs``,
            If True, return JSON converted object containing a list of
            revisions for this document. Structure of the returned object is
            defined by CouchDB. Refer to ``GET /<db>/<doc>`` in CouchDB API
            manual
        ``revs_info``,
            If True, return JSON converted object containing extended
            information about all revisions of the document. Structure of the
            returned object is defined by CouchDB.

        Note that, ``revs`` and ``revs_info`` object are cached using Etag header
        value.

        Admin-prev, No
        """
        rev = query.get( 'rev', None )
        revs = query.get( 'revs', None )
        revs_info = query.get( 'revs_info', None )
        conn, paths = self.conn, self.paths
        h = deepcopy( self.hthdrs )
        h.update( hthdrs )

        if rev != None and rev != self.doc['_rev'] :
            return self.__class__( self.db, self.doc, hthdrs=h, rev=rev )
        elif revs == True :
            q = { 'revs' : 'true' }
            if self.revs :
                s, h_, d = _headdoc( conn, paths, hthdrs=h, **q )
                if s == OK and h_['Etag'] == self.doc['_rev'] :
                    return self.revs
            s, h_, d = _readdoc( conn, paths, hthdrs=h, **q )
            self.revs = d
            return self.revs
        elif revs_info == True :
            q = { 'revs_info' : 'true' }
            if self.revs_info :
                s, h_, d = _headdoc( conn, paths, hthdrs=h, **q )
                if s == OK and h_['Etag'] == self.doc['_rev'] :
                    return self.revs_info
            s, h_, d = _readdoc( conn, paths, hthdrs=h, **q )
            self.revs_info = d
            return self.revs_info
        else :
            if self.doc :
                s, h_, d = _headdoc( conn, paths, hthdrs=h )
                if s == OK and h_['Etag'] == self.doc['_rev'] :
                    return self
            s, h_, d = _readdoc( conn, paths, hthdrs=h )
            self.doc = d
            return self

    def __repr__( self ) :
        _id = self.doc.get('_id', None)
        _rev = self.doc.get('_rev', None)
        return '<%s %r:%r>' % (type(self).__name__, _id, _rev)

    def items( self ) :
        """Return a list of (key,value) pairs in this document"""
        return self.doc.items()

    def keys( self ) :
        """Return a list of document keys"""
        return self.doc.keys()

    def all( self ) :
        """Shortcut for,

        >>> doc( revs=True )

        Returns JSON converted object containing a list of revisions for the
        document. Refer to ``GET /<db>/<doc>`` in CouchDB API manual.

        Admin-prev, No
        """
        return self( revs=True )

    def update( self, using={}, hthdrs={}, **query ) :
        """Update document ``using`` a dictionary.
        Updating the document using this interface will automatically PUT the
        document into the database. However, the following keys cannot be
        updated, ``_id``, ``_rev``. Optionally accepts HTTP headers `hthdrs`.
        Calling it with empty argument will simply put the existing document
        content into the database. To refresh the document object, do,
        `doc()`.

        Admin-prev, No
        """
        [ using.pop( k, None ) for k in ['_id', '_rev'] ]
        self.doc.update( using )
        conn, paths = self.conn, self.paths
        h = deepcopy( self.hthdrs )
        h.update( hthdrs )
        s, h, d = _updatedoc( conn, self.doc, paths, hthdrs=h, **query )
        self.doc.update({ '_rev' : d['rev'] }) if d else None
        return None

    def delitem( self, key ) :
        """Remove the specified key from document. However, the following keys
        cannot be removed, ``_id``, ``_rev``. To refresh the document object,
        do, `doc()`. Return the value of deleted key.

        Admin-prev, No
        """
        reserved = [ '_id', '_rev' ]
        if key in reserved : return None
        val, _ = ( self.doc.pop(key, None), self.update() )
        self.revs = None            # Enforce a fetch
        self.revs_info = None       # Enforce a fetch
        return val

    def copyto( self, toid, asrev=None ) :
        """Copy this revision of document to a destination specified by
        ``toid`` and optional revision ``asrev``. The source document revision
        will be same as this ``Document`` object. On success, return copied
        document object, else None.

        Admin-prev, No
        """
        return self.__class__.copy( self.db, self, toid, asrev=asrev,
                                    rev=self._rev )

    def addattach( self, filepath, content_type=None, hthdrs={}, **query ) :
        """Add file specified by ``filepath`` as attachment to this document.
        HTTP headers 'Content-Type' and 'Content-Length' will also be remembered
        in the database. Optionally, ``content_type`` can be provided as key-word
        argument.
        
        Return :class:`couchpy.attachment.Attachment` object.

        Admin-prev, No
        """
        filename = basename( filepath )
        data = open( filepath ).read()
        h = deepcopy( self.hthdrs )
        h.update( hthdrs )
        d = Attachment.putattachment(
                self.db, self, filepath, data, content_type=content_type,
                hthdrs=h, **query
            )
        self.doc.update({ '_rev' : d['rev'] }) if d != None else None
        return Attachment( self, filename )

    def delattach( self, attach, hthdrs={}, **query ) :
        """Delete the attachment specified either as filename or Attachment
        object, from this document.

        >>> doc.delattach( 'default.css' )
        >>> attach = doc.attach( 'default.css' )
        >>> doc.delattach( attach )
        """
        filename = attach.filename \
                   if isinstance(attach, Attachment) else attach
        h = deepcopy( self.hthdrs )
        h.update( self.hthdrs )
        d = Attachment.delattachment(self.db, self, filename, hthdrs=h, **query)
        self.doc.update({ '_rev' : d['rev'] }) if d != None else None
        return None

    def attach( self, filename ) :
        """Return :class:`couchpy.attachment.Attachment` object for
        ``filename`` attachment stored under this document."""
        a_ = self.doc.get( '_attachments', {} ).get( filename, None )
        a = Attachment( self, filename ) if a_ else None
        return a

    def attachs( self ) :
        """Return a list of all attachments in this document as 
        :class:`couchpy.attachment.Attachment` objects.
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
        """Probe whether the document is available"""
        id_ = doc if isinstance(doc, basestring) else doc['_id']
        paths = db.paths + [ id_ ]
        s, h, d = _headdoc( db.conn, paths, hthdrs=hthdrs, **query )
        return s, h, d

    @classmethod
    def create( cls, db, doc, attachfiles=[], hthdrs={}, fetch=True, **query ) :
        """Create a new document ``doc`` in the specified database ``db``.
        ``db`` must be :class:`couchpy.database.Database` object. ``doc`` will
        be converted to JSON structure before inserting it into the database.
        If ``doc`` has a key by name the ``_id``, then the document will be
        created with that ID, else, a new unique ID will be generated.
        To attach files in the document, pass a list of absolute filenames as
        key-word argument to ``attachfiles``. Attachments will be stored
        in the document using the file's `basename`.

        Optional arguments:

        ``fetch``,
            When true, the newly inserted document will be read back for its
            `_id, `_rev` and `_attachments` values.

        ``batch``,
            if specified 'ok', allow document store request to be batched
            with others.

        Return :class:`Document` object

        Admin-prev, No
        """
        id_ = [ doc['_id'] ] if '_id' in doc else []
        if not cls.validate_docid(id_) : 
            return None
        paths = db.paths
        attachs = Attachment.files2attach(attachfiles)
        if attachs :
            attachs_ = doc.get('_attachments', {})
            attachs_.update( attachs )
            doc['_attachments'] = attachs_
        s, h, d = _createdoc( db.conn, doc, paths, hthdrs, **query )
        if d == None : return None
        if fetch != True :
            doc.update({ '_id' : d['id'], '_rev' : d['rev'] })
        return Document( db, doc, fetch=fetch )

    @classmethod
    def delete( cls, db, doc, hthdrs={}, **query ) :
        """Delete a document ``doc`` from the specified database ``db``.
        ``db`` must be :class:`couchpy.database.Database` object.
        `doc` can be document-id or it can be :class:`Document` object,
        in which case the object is not valid after deletion.

        Optional arguments:

        ``rev``,
            the current revision of the document, if not specified, ``doc``
            must be a `Document` object whose ``_rev`` key will be used.

        Admin-prev, No
        """
        id_ = doc if isinstance(doc, basestring) else doc['_id']
        paths = db.paths + [ id_ ]
        q = query if isinstance(doc, basestring) else { 'rev' : doc['_rev'] } 
        s, h, d = _deletedoc( db.conn, paths, hthdrs, **q )
        return d

    @classmethod
    def copy( cls, db, doc, toid, asrev=None, hthdrs={}, **query ) :
        """Copy a source document to a destination, specified by
        ``toid`` and optional revision ``asrev``. If the source document's
        revision `rev` is not provided as key-word argument, then the latest
        revision of the document will be used.

        On success, return destination's `Document` object, else None.

        Admin-prev, No
        """
        id_ = doc if isinstance(doc, basestring) else doc['_id']
        dest = toid if asrev == None else "%s?rev=%s" % (toid, asrev)
        hthdrs = { 'Destination' : dest }
        paths = db.paths + [ id_ ]
        s, h, d = _copydoc( db.conn, paths, hthdrs=hthdrs, **query )
        if 'id' in d and 'rev' in d :
            return Document( db, d['id'], hthdrs=hthdrs, rev=d['rev'] )
        else :
            return None

    SPECIAL_DOC_NAMES = [
        '_all_docs',
        '_design',
        '_changes', '_compact',
        '_view_cleanup', '_temp_view',
        '_ensure_full_commit', '_bulk_docs', '_purge',
        '_missing_revs', '_revs_diff', '_revs_limit', 
        '_security',
        '_local',
    ]
    @classmethod
    def validate_docid( cls, docid ) :
        return not (docid in cls.SPECIAL_DOC_NAMES)



class LocalDocument( Document ) :

    def __init__( self, db, doc, fetch=True, hthdrs={}, **query ) :
        """Same as that of :func:`Document.__init__`, except that the
        document will be interperted as local to the database ``db``.

        Refer to, :func:`Document.__init__`
        """
        # TODO : url-encode _id ???
        self.db = db
        self.conn = db.conn

        id_ = doc if isinstance(doc, basestring) else doc['_id']
        id_ = self.id2name( id_ )
        self.paths = db.paths + [ '_local',  id_ ]

        s, h, doc = _readdoc( self.conn, self.paths, hthdrs=hthdrs, **query
                    ) if fetch == True else (None, None, doc )
        self.doc = doc
        self.revs = None        # Cached object
        self.revs_info = None   # Cached object
        self.client = db.client
        self.debug = db.debug
        self.hthdrs = hthdrs

    @classmethod
    def create( cls, db, doc, attachfiles=[], hthdrs={}, fetch=True, **query ) :
        """Same as that of :func:`Document.create`, except that the
        document will be interperted as local to the database ``db``. ``doc``
        must be dictionary of key,value pairs.

        Refer to, :func:`Document.create`
        """
        id_ = doc['_id']
        if not cls.validate_docid(id_) : 
            return None
        paths = db.paths + [ '_local', id_ ]
        attachs = Attachment.files2attach(attachfiles)
        if attachs :
            attachs_ = doc.get('_attachments', {})
            attachs_.update( attachs )
            doc['_attachments'] = attachs_
        s, h, d = _updatedoc( db.conn, doc, paths, hthdrs, **query )
        if d == None : return None
        if fetch != True :
            doc.update({ '_rev' : d['rev'] })
        return LocalDocument( db, doc, fetch=fetch )

    @classmethod
    def delete( cls, db, doc, hthdrs={}, **query ) :
        """Same as that of :func:`Document.delete`, except that the
        document will be interperted as local to the database ``db``.

        Refer to, :func:`Document.delete`
        """
        id_ = doc if isinstance(doc, basestring) else doc['_id']
        id_ = id_[7:] if id_.startswith( '_local/' ) else id_
        paths = db.paths + [ '_local', id_ ]
        q = query if isinstance(doc, basestring) else { 'rev' : doc['_rev'] } 
        s, h, d = _deletedoc( db.conn, paths, hthdrs, **q )
        return d

    @classmethod
    def copy( cls, db, doc, toid, asrev=None, hthdrs={}, **query ) :
        """Same as that of :func:`Document.copy`, except that the
        document will be interperted as local to the database ``db``.

        Refer to, :func:`Document.copy`
        """
        id_ = doc if isinstance(doc, basestring) else doc['_id']
        dest = toid if asrev == None else "_local/%s?rev=%s" % (toid, asrev)
        hthdrs = { 'Destination' : dest }
        id_ = id_[7:] if id_.startswith( '_local/' ) else id_
        paths = db.paths + [ '_local', id_ ]
        s, h, d = _copydoc( db.conn, paths, hthdrs=hthdrs, **query )
        if 'id' in d and 'rev' in d :
            id_ = d['id']
            id_ = id_[7:] if id_.startswith( '_local/' ) else id_
            return LocalDocument( db, id_, hthdrs=hthdrs )
        else :
            return None

    def id2name( self, id_ ) :
        return id_[6:] if id_.startswith( '_local' ) else id_
