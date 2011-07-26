"""CouchDB is a document database and the documents are stored in JSON format.
Fortunately, JSON formated objects can easily be converted to native python
objects. :class:`couchpy.database.Document` class defines a collection of
attributes and methods to access CouchDB documents.

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
>>> doc.attachs()                                 # Get a list of Attachment objects
>>> a = doc.attach( 'recipe.txt' )
>>> a.filename                                    # Attachment filename 
receipe.txt
>>> a.data()
( ... file content ..., text/plain )
>>> doc.delattach( a )                            # Delete attachment

Delete document,

>>> Document.delete( db, doc1 )

Copy document,

>>> bkpdoc = Document.copy( db, doc._id, 'Fishstew-bkp', rev=doc._rev )

"""

import sys, re, json, time, logging, base64
from   os.path      import basename
from   copy         import deepcopy
from   StringIO     import StringIO
from   mimetypes    import guess_type

import rest
from   httperror    import *
from   httpc        import HttpSession, ResourceNotFound, OK, CREATED, ACCEPTED
from   couchpy      import CouchPyError, hdr_acceptjs, hdr_ctypejs
from   mixins       import MixinDoc

# TODO :
#   1. Batch mode POST / PUT should have a verification system built into it.
#   2. Attachments allowed in local documents ???

log = logging.getLogger( __name__ )

def _postdoc( conn, doc, paths=[], hthdrs={}, **query ) :
    """POST /<db>/<doc>
    query,
        batch='ok'
    """
    body = rest.data2json( doc )
    batch = query.get( 'batch', None )
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    s, h, d = conn.post( paths, hthdrs, body, _query=query.items() )
    if batch == 'ok' and s == ACCEPTED and d['ok'] == True :
        return s, h, d
    elif s == CREATED and d['ok'] == True :
        return s, h, d
    else :
        log.error( 'POST request to /%s failed' % '/'.join(paths) )
        return (None, None, None)

def _getdoc( conn, paths=[], hthdrs={}, **query ) :
    """
    GET /<db>/<doc>
    GET /<db>/_local/<doc>
    query,
        rev=<_rev>, revs=<'true'>, revs_info=<'true'>
    """
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    s, h, d = conn.get( paths, hthdrs, None, _query=query.items() )
    if s == OK :
        return s, h, d
    else :
        log.error( 'GET request to /%s failed' % '/'.join(paths) )
        return (None, None, None)

def _headdoc( conn, paths=[], hthdrs={}, **query ) :
    """
    HEAD /<db>/<doc>
    HEAD /<db>/_local/<doc>
    query,
        rev=<_rev>, revs=<'true'>, revs_info=<'true'>
    """
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    s, h, d = conn.head( paths, hthdrs, None, _query=query.items() )
    if s == OK :
        return s, h, d
    else :
        log.error( 'HEAD request to /%s failed' % '/'.join(paths) )
        return (None, None, None)

def _putdoc( conn, doc, paths=[], hthdrs={}, **query ) :
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
    batch = query.get( 'batch', None )
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    s, h, d = conn.put( paths, hthdrs, body, _query=query.items() )
    if batch == 'ok' and s == ACCEPTED and d['ok'] == True :
        return s, h, d
    elif s == CREATED and d['ok'] == True :
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
    if query.get( 'rev', None ) is None :
        raise CouchPyError( '`rev` to be supplied while deleteing the doc' )
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
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
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    s, h, d = conn.copy( paths, hthdrs, None, _query=query.items() )
    if s == CREATED :
        return s, h, d
    else :
        return (None, None, None)


# Document states
ST_ACTIVE_INVALID  = 10     #(ACTIVE, 'invalid')
ST_ACTIVE_VALID    = 11     #(ACTIVE, 'valid')
ST_ACTIVE_DIRTY    = 12     #(ACTIVE, 'dirty')
ST_ACTIVE_POST     = 13     #(ACTIVE, 'post')
ST_CACHE_INVALID   = 14     #(CACHE,  'invalid')

ST_EVENT_FETCH     = 100    # document fetch() call
ST_EVENT_POST      = 101    # document post() call
ST_EVENT_PUT       = 102    # document put() call
ST_EVENT_DELETE    = 103    # document delete() call
ST_EVENT_SIDEEFF   = 104    # document side-effects
ST_EVENT_AGET      = 105    # document attach.get() call
ST_EVENT_APUT      = 106    # document attach.put() call
ST_EVENT_ADELETE   = 107    # document attach.delete() call
ST_EVENT_ATTACH    = 108    # document attach() call
ST_EVENT_INSTAN    = 109    # Document()

class StateMachine( object ):

    def __init__( self, doc ):
        self.doc = doc

    def handle_event( self, event, *args, **kwargs ):
        return self.events[event]( self, *args, **kwargs )

    def is_allowed( self, event, doc ):
        state = doc._x_state
        if event == ST_EVENT_POST and state == ST_ACTIVE_POST :
            return True
        if event == ST_EVENT_PUT and state == ST_ACTIVE_DIRTY :
            return True
        return False

    def event_instan( self, *args, **kwargs ):      # ST_EVENT_INSTAN
        db, _doc  = args[0:2]
        doc = self.doc
        activedocs = db.singleton_docs['active']
        cacheddocs = db.singleton_docs['cache']
        _id, _rev  = _doc.get('_id', None), _doc.get('_rev', None)
        _x_state   = doc.get( '_x_state', None )

        if _x_state == None :                           # Fresh instantiation
            activedocs[_id] = doc
            doc._x_init = True
            newstate = ST_ACTIVE_POST if _rev == None else ST_ACTIVE_INVALID

        elif _id and doc._x_state == ST_CACHE_INVALID : # `cache` to `active`
            activedocs[_id] = cacheddocs.pop( _id )
            doc._x_reinit = True
            newstate = ST_ACTIVE_INVALID

        elif _id and doc._x_state not in [ ST_ACTIVE_DIRTY, ST_ACTIVE_POST ] :
            doc._x_reinit = True                        # Repeated intantiation
            newstate = ST_ACTIVE_INVALID

        else :
            newstate = _x_state

        doc._x_state = newstate

    def event_side_effect( self, doc ):                 # ST_EVENT_SIDEEFF
        _x_state = doc._x_state
        if _x_state == ST_ACTIVE_INVALID :
            doc.fetch()
            newstate = ST_ACTIVE_DIRTY
        elif _x_state == ST_ACTIVE_VALID :
            newstate = ST_ACTIVE_DIRTY
        elif _x_state in [ ST_ACTIVE_DIRTY, ST_ACTIVE_POST ] :
            newstate = _x_state
        elif _x_state == ST_CACHE_INVALID :
            raise Exception( 'Side effects not allowed in CACHE_INVALID state' )
        doc._x_state = newstate

    def event_fetch( self, doc, dbdoc ):                # ST_EVENT_FETCH
        _x_state = doc._x_state
        if _x_state in [ ST_ACTIVE_POST, ST_ACTIVE_INVALID, ST_ACTIVE_VALID ] :
            doc.clear( _x_dirty=False )
            doc.update( dbdoc, _x_dirty=False )
            newstate = ST_ACTIVE_VALID
        else :
            err = 'Cannot fetch dirtied documents / cached documents'
            raise Exception( err )
        doc._x_state = newstate

    def event_post( self, doc, d ):                     # ST_EVENT_POST
        _x_state = doc._x_state
        if _x_state == ST_ACTIVE_POST :
            if 'rev' in d :
                doc.update( _rev=d['rev'], _x_dirty=False )
                newstate = ST_ACTIVE_VALID
            else :
                newstate = ST_ACTIVE_INVALID
        else :
            raise Exception( 'Document might be instantiated with `_rev` field' )
        doc._x_state = newstate

    def event_put( self, doc, d ):                      # ST_EVENT_PUT
        _x_state = doc._x_state
        if _x_state == ST_ACTIVE_DIRTY :
            if 'rev' in d :
                doc.update( _rev=d['rev'], _x_dirty=False )
                newstate = ST_ACTIVE_VALID
            else :
                newstate = ST_ACTIVE_INVALID
        else :
            err = 'Document cannot be updated, may be no changes made'
            raise Exception( err )
        doc._x_state = newstate

    def event_delete( self, doc, d ):                   # ST_EVENT_DELETE
        _x_state = doc._x_state
        db       = doc._x_db
        if _x_state in [ ST_ACTIVE_VALID, ST_ACTIVE_INVALID, ST_ACTIVE_POST ] :
            doc.update( _rev=d['rev'], _x_dirty=False )
            db.singleton_docs['active'].pop(doc._id) # Neiher active nor cached
        else :
            raise Exception( 'Document cannot be deleted' )

    def event_attach( self, doc ):                      # ST_EVENT_ATTACH
        _x_state = doc._x_state
        if _x_state != ST_ACTIVE_POST :
            raise Exception('_attachments can be added only for new documents')

    def event_aget( self, doc ):                        # ST_EVENT_AGET
        _x_state = doc._x_state
        if _x_state not in [ST_ACTIVE_DIRTY, ST_ACTIVE_INVALID, ST_ACTIVE_VALID]:
            raise Exception( 'Cannot get attachment for fresh documents' )

    def event_aput( self, doc, d ):                     # ST_EVENT_APUT
        _x_state = doc._x_state
        if _x_state not in [ST_ACTIVE_POST, ST_CACHE_INVALID ] :
            doc.update( _rev=d['rev'], _x_dirty=False )
        else :
            raise Exception( 'Cannot put attachments for fresh documents' )

    def event_adelete( self, doc, d ):                  # ST_EVENT_ADELETE
        _x_state = doc._x_state
        if _x_state not in [ST_ACTIVE_POST, ST_CACHE_INVALID ] :
            doc.update( _rev=d['rev'], _x_dirty=False )
        else :
            raise Exception( 'Cannot delete attachments for fresh documents' )
            

    events = {
        ST_EVENT_INSTAN  : event_instan,
        ST_EVENT_SIDEEFF : event_side_effect,
        ST_EVENT_FETCH   : event_fetch,
        ST_EVENT_POST    : event_post,
        ST_EVENT_PUT     : event_put,
        ST_EVENT_DELETE  : event_delete,
        ST_EVENT_ATTACH  : event_attach,
        ST_EVENT_AGET    : event_aget,
        ST_EVENT_APUT    : event_aput,
        ST_EVENT_ADELETE : event_adelete,
    }


class Document( dict ) :
    """Document modeling.

    Instantiate python representation of a CouchDB document. The resulting
    object is essentially a dictionary class. ``db`` should be a
    :class:`couchpy.database.Database` object, while ``doc`` can be one of the
    following, which will change the behaviour of the instantiation, along
    with the rest of the arguments.

    ``doc`` can be ``_id`` string,

        in which case, a call to the fetch() method will get the latest
        version of the document from the server. When an attempt is made to
        change / update the document content, the latest version will be
        automatically fetched from the server (if it is not already fetched).

    ``doc`` can be dictionary,

        if `_id` key and `_rev` key is present (which is the latest revision
        of the document), then the document instantiation behaves exactly the
        same way as if `doc` is `_id` string.

        if `_id` key is present but `_rev` key is not present or if both `id`
        key and `rev` keys are not present, then the document instance is
        presumed as a fresh document which needs to be created. The actual
        creation happens when :func:`couchpy.doc.post` method is called. 

    ``doc`` with ``rev`` keyword parameter,
        
        When instantiation happens for an existing document and ``rev``
        keyword parameter is provided, then it means that ``rev`` points to an
        older document and the instance will be an immutable one. The only
        purpose for such instances are to fetch() the document from database
        and consume its content.

    Optional arguments:

    ``hthdrs``,
        HTTP headers which will be remembered for all database access
        initiated via this object.

    ``rev``,
        Specify document's revision to fetch, will instantiate an immutable
        version of the document.

    ``revs``,
        If True, then, the document will include a revision list for this
        document. To learn more about the structure of the returned object,
        refer to ``GET /<db>/<doc>`` in CouchDB API manual

    ``revs_info``,
        If True, then, the document will include extended revision list for
        this document. To learn more about the structure of the returned object,
        refer to ``GET /<db>/<doc>`` in CouchDB API manual

    Admin-prev, No
    """

    def __new__( cls, *args, **kwargs ) :
        """Document instantiater. If `_id` is provided, the singleton object
        is lookedup from 'active' list or 'cached' list. If doc is not
        present, a new :class:`couchpy.doc.Document` is instantiated which
        will be saved in the 'active' list if the document `_id` is available.
        Sometimes, while creating a new document, the caller may not provide
        the `_id` value (DB will autogenerate the ID). In those scenarios, the
        document instance will be added to the 'active' list after a
        :func:`couchpy.doc.Document.post` method is called.
        """
        # Fix `_id` parameter
        args = list(args)
        args[1] = {'_id' : args[1]} if isinstance(args[1], basestring) else args[1]
        #----
        db, doc = args[:2]
        _id     = doc.get( '_id', None )
        activedocs = db.singleton_docs['active']
        cacheddocs = db.singleton_docs['cache']

        # Instantiate document's older revision, ImmutableDocument.
        if _id and 'rev' in kwargs :
            self = dict.__new__( ImmutableDocument, *args, **kwargs )
            self.__init__( *args, **kwargs )
            return self

        if _id and _id in activedocs :                      # Document is active
            self = activedocs[_id]

        elif _id and _id in cacheddocs :                    # Document is cached
            self = cacheddocs[_id]

        else :                                         
            self = dict.__new__( cls, *args, **kwargs )     # Make new instance
            self._x_smach = StateMachine( self )

        # State machine
        self._x_smach.handle_event( ST_EVENT_INSTAN, *args, **kwargs )

        # The instance can be in any of the active state.
        return self

    def __init__( self, db, doc, hthdrs={}, **query ) :
        doc = {'_id' : doc} if isinstance(doc, basestring) else doc
        _id       = doc.get( '_id', None )
        _x_init   = getattr(self, '_x_init', None)
        _x_reinit = getattr(self, '_x_reinit', None)
        if _x_init == True :
            self._x_db, self._x_conn = db, db.conn
            self._x_paths  = db.paths + ( [_id] if _id else [] )
            self._x_query  = {}
        if _x_init or _x_reinit :
            self._reinitialize( db, doc, hthdrs=hthdrs, **query )
        self._x_init = self._x_reinit = False

        dict.__init__( self, doc )

    def _reinitialize( self, *args, **kwargs ) :
        hthdrs = kwargs.pop( 'hthdrs', {} )
        self._x_hthdrs = self._x_conn.mixinhdrs( self._x_db.hthdrs, hthdrs )
        self._x_query.update( kwargs )

    #---- Dictionary methods that create side-effects

    def __getattr__( self, name ) :
        """Access document values as attributes of this instance."""
        if name in self :
            return self[name]
        else :
            raise AttributeError( 'accessing %r' % name )

    def __setattr__( self, name, value ) :
        """Set document values as attributes to this instance"""
        if name.startswith('_x_') :
            self.__dict__[name] = value
        else :
            self[name] = value
        return value

    def __setitem__(self, key, value) :
        """Intercept dictionary updates (which are updates to the document as
        well) and mark the dirty fields.
        """
        self._x_smach.handle_event( ST_EVENT_SIDEEFF, self )
        dict.__setitem__( self, key, value )

    def __delitem__( self, key ) :
        """Delete key,value pair identified by ``key``. Python shortcut for
        :func:`Document.delitem` also mark them for later commit"""
        self._x_smach.handle_event( ST_EVENT_SIDEEFF, self )
        return dict.__delitem__( self, key )

    def clear( self, _x_dirty=True ):
        self._x_smach.handle_event(ST_EVENT_SIDEEFF, self) if _x_dirty else None
        return dict.clear( self )

    def update( self, *args, **kwargs ):
        _x_dirty = kwargs.pop( '_x_dirty', True )
        self._x_smach.handle_event(ST_EVENT_SIDEEFF, self) if _x_dirty else None
        return dict.update( self, *args, **kwargs )

    def setdefault( self, key, *args ):
        self._x_smach.handle_event( ST_EVENT_SIDEEFF, self )
        return dict.setdefault( self, key *args )

    def pop( self, key, *args ):
        self._x_smach.handle_event( ST_EVENT_SIDEEFF, self )
        return dict.pop( self, key, *args )

    def popitem( self ):
        self._x_smach.handle_event( ST_EVENT_SIDEEFF, self )
        return dict.popitem( self )

    def __call__( self, hthdrs={}, **query ) :
        """Behaves like a factory method returning Document instances based on
        the keyword parameters

        Optional keyword arguments:

        ``hthdrs``,
            HTTP headers to be used for the document instance.

        ``revs``,
            When fetching the document, include revision information.

        ``revs_info``,
            When fetching the document, include extended revision information.

        ``rev``,
            If specified, `rev` will be assumed as one of the previous revision
            of this document. An immutable version of document
            (ImmutableDocument) will be returned.

        Admin-prev, No
        """
        doc = dict( self.items() )
        return self.__class__( self._x_db, doc, hthdrs=hthdrs, **query )

    def __repr__( self ):
        _id = self.get('_id', None)
        _rev = self.get('_rev', None)
        return '<%s %r:%r>' % (type(self).__name__, _id, _rev)

    #---- HTTP methods for Document instance

    def head( self, hthdrs={}, **query ):
        """HEAD method to check for the presence and latest revision of
        this document in the server.

        Optional keyword parameters,

        ``hthdrs``,
            HTTP headers for this HTTP request.

        ``rev``,
            Make the HEAD request for this document's revision, `rev`

        ``revs``,
            Make the HEAD request for this document's revisions

        ``revs_info``,
            Make the HEAD request for this document's revisions

        Returns HTTP response header

        Admin-prev, No
        """
        conn, paths = self._x_conn, self._x_paths
        hthdrs = conn.mixinhdrs( self._x_hthdrs, hthdrs )
        s, h, d = _headdoc( conn, paths, hthdrs=hthdrs, **query )
        return h

    def post( self, hthdrs={}, **query ):
        """POST method on this document. To create (insert) new a document into
        the database, create and instance of :class:`couchpy.doc.Document`
        without specifying the `_rev` field and call this `post` method on the
        instance. If `_id` is not provided, CouchDB server will create an id
        value for the document. New documents are not created until a call is
        made to this method.

        Optional keyword parameters,

        ``hthdrs``,
            HTTP headers for this HTTP request.

        ``batch``,
            if specified 'ok', allow document store request to be batched
            with others. When using the batch mode, the document instance will
            not be updated with the latest revision number. A `fetch()` call
            is required to get the latest revision of the document.

        Returns the document with its `_rev` field updated to the latest
        revision number, along with the `_id` field.

        Admin-prev, No
        """
        if self._x_smach.is_allowed( ST_EVENT_POST, self ) == False :
            raise Exception( 'post() not allowed !!' )

        conn, paths = self._x_conn, self._x_paths
        # Prune away the document ID from url path. POST will crib on that.
        if paths[-1] == self.get( '_id', None ) :
            paths = paths[:-1]
        hthdrs = conn.mixinhdrs( self._x_hthdrs, hthdrs )
        doc = dict( self.items() )
        s, h, d = _postdoc( conn, doc, paths, hthdrs=hthdrs, **query )
        self._x_smach.handle_event( ST_EVENT_POST, self, d ) if d else None
        return self

    def fetch( self, hthdrs={}, **query ):
        """GET the document from disk. Always fetch the latest revision of
        the document. Documents are not fetched from the database until a call
        is made to this document.

        Optional keyword parameters,

        ``hthdrs``,
            HTTP headers for this HTTP request.

        ``revs``,
            Get the document with a list of all revisions on the disk.

        ``revs_info``,
            Get the document with a list of extended revision infor.

        Returns this document object.

        Admin-prev, No
        """
        conn, paths = self._x_conn, self._x_paths
        hthdrs = conn.mixinhdrs( self._x_hthdrs, hthdrs )
        q      = conn.mixinhdrs( self._x_query, query )
        s, h, doc = _getdoc( conn, paths, hthdrs=hthdrs, **q )
        self._x_smach.handle_event( ST_EVENT_FETCH, self, doc ) if doc else None
        return self

    def put( self, hthdrs={}, **query ) :
        """If the document instance is changed / modified, persist the changes
        in the server by calling this method. This method can be called only
        for documents that already exist in database. Documents are not
        updated (persisted) in the database, until a call is made to this
        method.

        Optional keyword parameters,

        ``batch``,
            if specified 'ok', allow document store request to be batched
            with others. When using the batch mode, the document instance will
            not be updated with the latest revision number. A `fetch()` call
            is required to get the latest revision of the document.

        Returns the document with its `_rev` field updated to the latest
        revision number.

        Admin-prev, No
        """
        if self._x_smach.is_allowed( ST_EVENT_PUT, self ) == False :
            raise Exception( 'put() is allowed only on dirtied document !!' )

        conn, paths = self._x_conn, self._x_paths
        hthdrs = conn.mixinhdrs( self._x_hthdrs, hthdrs )
        doc = dict( self.items() )
        s, h, d = _putdoc( conn, doc, paths, hthdrs=hthdrs, **query )
        self._x_smach.handle_event( ST_EVENT_PUT, self, d ) if d else None
        return self

    def delete( self, hthdrs={}, rev=None ) :
        """Delete the document from database. The document and its instance
        will no more be managed by CoucPy.

        Optional keyword parameters,

        ``rev``,
            Latest document revision.

        Returns None.

        Admin-prev, No
        """
        conn, paths = self._x_conn, self._x_paths
        rev = rev or self['_rev']
        hthdrs = conn.mixinhdrs( self._x_hthdrs, hthdrs )
        s, h, d = _deletedoc( conn, paths, hthdrs=hthdrs, rev=rev )
        if d and 'rev' in d :
            self._x_smach.handle_event( ST_EVENT_DELETE, self, d )
        return None

    def copy( self, toid, asrev=None, hthdrs={} ) :
        """Copy this document to a new document, the source document's id and
        revision will be interpreted from this object, while the destination's
        id and revision must be specified via the keyword argument
        
        ``toid``,
            Destination document id

        ``asrev``,
            Destination document's latest revision

        Return the copied document object
        """
        conn, paths = self._x_conn, self._x_paths
        dest = toid if asrev == None else "%s?rev=%s" % (toid, asrev)
        hthdrs = conn.mixinhdrs(self._x_hthdrs, hthdrs, {'Destination' : dest})
        rev = self['_rev']
        s, h, d = _copydoc( conn, paths, hthdrs=hthdrs, rev=rev )
        if d :
            doc = { '_id' : d['id'], '_rev' : d['rev'] }
            return self.__class__( self._x_db, doc )
        else :
            return None

    def attach( self, filepath, content_type=None ):
        """Add files to document before posting it (creating the document).
        something like this
        >>> doc = db.Document( db, { '_id' : 'FishStew' } )
        >>> doc.attach( '/home/user/readme.txt' ).post()

        ``filepath``,
            Compute filename and file content from this.

        optional keyword arguments,

        ``content_type``,
            File's content type.
        """
        self._x_smach.handle_event( ST_EVENT_ATTACH, self )

        filename = basename(filepath)
        ctype = content_type if content_type else guess_type(filename)[0]
        attachments = self.get('_attachments', [])
        attachments.append({
            'filename'     : filename,
            'content_type' : ctype,
            'data'         : base64.encodestring( open(filepath).read() ),
        })
        self.update( _attachments=attachments, _x_dirty=False )
        return self

    def attachments( self ) :
        return [ self.Attachment( filename=filename, **fields )
                 for filename, fields in self._attachments.items() ]

    def Attachment( self, *args, **kwargs ):
        return Attachment( self, *args, **kwargs )



#---- Attachment APIs

def _getattach( conn, paths=[], hthdrs={} ) :
    """
    GET /<db>/<doc>/<attachment>
    GET /<db>/_design/<design-doc>/<attachment>
    """
    s, h, d = conn.get( paths, hthdrs, None )
    if s == OK :
        return s, h, d
    else :
        log.error( 'GET request to /%s failed' % '/'.join(paths) )
        return (None, None, None)

def _putattach( conn, paths=[], body='', hthdrs={}, **query ) :
    """
    PUT /<db>/<doc>/<attachment>
    PUT /<db>/_design/<design-doc>/<attachment>
    query,
        rev=<_rev>, current revision of the document
    """
    if 'Content-Length' not in hthdrs :
        raise CouchPyError( '`Content-Length` header field not supplied' )
    if 'Content-Type' not in hthdrs :
        raise CouchPyError( '`Content-Type` header field not supplied' )
    s, h, d = conn.put( paths, hthdrs, body, _query=query.items() )
    if s == CREATED and d['ok'] == True :
        return s, h, d
    else :
        log.error( 'GET request to /%s failed' % '/'.join(paths) )
        return (None, None, None)

def _deleteattach( conn, paths=[], hthdrs={}, **query ) :
    """
    DELETE /<db>/<doc>/<attachment>
    DELETE /<db>/_design/<design-doc>/<attachment>
    query,
        rev=<_rev>, current revision of the document
    """
    s, h, d = conn.delete( paths, hthdrs, None, _query=query.items() )
    if s == OK and d['ok'] == True :
        return s, h, d
    else :
        log.error( 'GET request to /%s failed' % '/'.join(paths) )
        return (None, None, None)



class Attachment( object ) :
    """Represents a single attachment file present in the document, allows
    operations like put / get / delete of the attachment file under the
    document. Note that these methods are applicable only for documents that
    are already inserted (created) in the database.
    :class:`couchpy.doc.Attachment` object must be instantiated under a
    :class:`couchpy.doc.Document` context. The natural way to do that is,
    >>> doc = db.Document( db, { '_id' : 'FishStew' } ).fetch()
    >>> doc.Attachment( filepath='/home/user/readme.txt' ).put()

    Instances of attachments are also emitted by document objects like,
    >>> doc = db.Document( db, { '_id' : 'FishStew' } ).fetch()
    >>> attachs = doc.attachments() # List of `Attachment` objects
    
    Add file specified by ``filepath`` as attachment to this document.
    HTTP headers 'Content-Type' and 'Content-Length' will also be remembered
    in the database. Optionally, ``content_type`` can be provided as key-word
    argument.
    
    Return :class:`couchpy.attachment.Attachment` object.

    Admin-prev, No
    """

    def __init__( self, doc, hthdrs={}, filename=None, filepath=None, **fields ):
        self.doc = doc
        self.hthdrs = self.conn.mixinhdrs( self.doc._x_db.hthdrs, hthdrs )

        [ setattr(self, k, v) for k,v in fields.items() ]
        self.filepath = filepath
        self.filename = filename or basename( filepath )
        self.content_type = fields.get('content_type', guess_type(filename)[0])
        self.data = base64.encodestring( open( self.filepath ).read() 
                    ) if self.filepath else None
        self.paths = self.doc.paths + [ filename ]
        self.hthdrs.update({ 'Content-Type' : self.content_type })
        
    def get( self ) :
        """GET attachment from database. Attributes like, `file_name`,
        `content_type`, `data` are available on this object.
        """
        conn, paths = self.doc._x_conn, self.paths
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _getattach( conn, paths, hthdrs=hthdrs )
        if s != None :
            self.content_type = h.get( 'Content-Type', None )
            self.data = d
            self.doc._x_smach.handle_event( ST_EVENT_AGET, self.doc )
        return self

    def put( self ) :
        """Upload the attachment. Attachments are not added to the document
        until a call is made to this method. Uploading attachment will
        increment the revision number of the document, which will be
        automatically updated in the attachment's document instance."""
        conn, paths = self.doc._x_conn, self.paths
        self.fetch( force=False )
        rev = self.doc['_rev']
        s, h, d = _putattach( conn, paths, self.data, hthdrs=self.hthdrs, rev=rev )
        if d and 'rev' in d :
            self.doc._x_smach.handle_event( doc, d, ST_EVENT_APUT )
        return self

    def delete( self ) :
        """Delete the attachment file from the document. This will increment
        the revision number of the document, which will be automatically
        updated in the attachment's document instance"""
        conn, paths = self.doc._x_conn, self.paths
        self.fetch( force=False )
        rev = self.doc['_rev']
        s, h, d = _deleteattach( conn, paths, hthdrs=self.hthdrs, rev=rev )
        if d and 'rev' in d :
            self.doc._x_smach.handle_event( self.doc, d, ST_EVENT_ADELETE )
        return self


#---- Local documents

class LocalDocument( dict ) :
    """Local documents have the following limitations:
      * Local documents are not replicated to other databases.
      * The ID of the local document must be known for the document to accessed.
        You cannot obtain a list of local documents from the database.
      * Local documents are not output by views, or the _all_docs view.
    Note that :class:`couchpy.doc.LocalDocument` class is not derived from
    :class:`couchpy.doc.Document` class.

    Optional keyword arguments:

    ``hthdrs``,
        HTTP headers which will be remembered for all database access
        initiated via this object.

    ``rev``,
        Specify local document's revision to fetch.

    ``revs``,
        If True, then, the document will include a revision list for this local
        document. To learn more about the structure of the returned object,
        refer to ``GET /<db>/<doc>`` in CouchDB API manual

    ``revs_info``,
        If True, then, the document will include extended revision list for
        this local document. To learn more about the structure of the returned
        object, refer to ``GET /<db>/<doc>`` in CouchDB API manual

    Admin-prev, No
    """

    def __init__( self, db, doc, hthdrs={}, **query ) :
        doc = {'_id' : doc} if isinstance(doc, basestring) else doc
        _id = doc['_id']
        self._x_db, self._x_conn = db, db.conn
        self._x_paths  = db.paths + [ '_local', _id ]
        self._x_hthdrs = self._x_conn.mixinhdrs( db.hthdrs, hthdrs )
        self._x_query  = query

        dict.__init__( self, doc )

    #---- Dictionary methods that create side-effects

    def __getattr__( self, name ) :
        """Access local document values as attributes of this instance."""
        if name in self :
            return self[name]
        else :
            raise AttributeError( 'accessing %r' % name )

    def __setattr__( self, name, value ) :
        """Set local document values as attributes to this instance"""
        if name.startswith('_x_') :
            self.__dict__[name] = value
        else :
            self[name] = value
        return value

    def __call__( self, hthdrs={}, **query ) :
        """Behaves like a factory method generating LocalDocument instance
        based on the keyword arguments.
        
        Optional keyword arguments:

        ``hthdrs``,
            HTTP headers to be used for the document instance.

        ``revs``,
            When fetching this document, include revision information.

        ``revs_info``,
            When fetching this document, include extended revision information.

        ``rev``,
            When fetching, return the local document for the requested
            revision.

        Admin-prev, No
        """
        doc = dict( self.items() )
        return self._x_db.LocalDocument( self, doc, hthdrs=hthdrs, **query )

    def fetch( self, hthdrs={}, **query ):      # Local document
        """GET this local document from disk. Always fetch the latest revision of
        the document.

        Optional keyword parameters,

        ``hthdrs``,
            HTTP headers for this HTTP request.

        ``revs``,
            Get the document with a list of all revisions on the disk.

        ``revs_info``,
            Get the document with a list of extended revision infor.

        Returns this document object.

        Admin-prev, No
        """
        conn, paths = self._x_conn, self._x_paths
        hthdrs = conn.mixinhdrs( self._x_hthdrs, hthdrs )
        q      = conn.mixinhdrs( self._x_query, query )
        s, h, ldoc = _getdoc( conn, paths, hthdrs=hthdrs, **q )
        if ldoc :
            self.clear()
            self.update( ldoc )
        return self

    def put( self, hthdrs={}, **query ) :
        """Persist the local document on the disk.

        Optional keyword parameters,

        ``batch``,
            if specified 'ok', allow document store request to be batched
            with others.

        Returns the document with its `_rev` field updated to the latest
        revision number.

        Admin-prev, No
        """
        conn, paths = self._x_conn, self._x_paths
        hthdrs = conn.mixinhdrs( self._x_hthdrs, hthdrs )
        doc = dict( self.items() )
        s, h, d = _putdoc( conn, doc, paths, hthdrs=hthdrs, **query )
        self.update( _rev=d['rev'] ) if d and 'rev' in d else None
        return self

    def delete( self, hthdrs={}, rev=None ) :
        """Delete the local document from the database.

        Optional keyword parameters,

        ``rev``,
            Latest document revision.

        Returns None.

        Admin-prev, No
        """
        conn, paths = self._x_conn, self._x_paths
        rev = rev or self['_rev']
        hthdrs = conn.mixinhdrs( self._x_hthdrs, hthdrs )
        s, h, d = _deletedoc( conn, paths, hthdrs=hthdrs, rev=rev )
        self.update( _rev=d['rev'] ) if d and 'rev' in d else None
        return None

    def copy( self, toid, asrev=None, hthdrs={} ) :
        """Copy this local document to a new document, the source document's id
        and revision will be interpreted from this object, while the
        destination's id and revision must be specified via the keyword
        argument.

        ``toid``,
            Destination document id

        ``asrev``,
            Destination document's latest revision

        Return the copied document object
        """
        conn, paths = self._x_conn, self._x_paths
        dest = toid if asrev == None else "%s?rev=%s" % (toid, asrev)
        hthdrs = conn.mixinhdrs(self._x_hthdrs, hthdrs, {'Destination' : dest})
        # TODO : `rev` is not being accepted ???
        rev = self['_rev']
        s, h, d = _copydoc( conn, paths, hthdrs=hthdrs )
        if d :
            doc = { '_id' : d['id'], '_rev' : d['rev'] }
            return LocalDocument( self._x_db, doc )
        else :
            return None


class ImmutableDocument( dict ):
    """Immutable version of document objects, the document must ve specified
    with `_id` and `_rev`. Users cannot change or modify the document
    contents. Unlike the :class:`couchpy.doc.Document` objects, only
    :func:`couchpy.doc.ImmutableDocument.fetch` method is available.

    Optional keyword arguments,

    ``hthdrs``,
        HTTP headers for this HTTP request.

    ``rev``,
        Specify document's revision to fetch.

    ``revs``,
        Get the document with a list of all revisions on the disk.

    ``revs_info``,
        Get the document with a list of extended revision infor.
    """

    def __init__( self, db, doc, hthdrs={}, rev=None, **query ) :
        doc = {'_id' : doc} if isinstance(doc, basestring) else doc
        if rev != None :
            dict.update( self, _rev=rev )
        self._x_db, self._x_conn = db, db.conn
        self._x_hthdrs, self._x_query = hthdrs, query
        self._x_paths  = db.paths + [ doc['_id'] ]
        dict.__init__( self, doc )

    def __getattr__( self, name ) :
        if name in self :
            return self[name]
        else :
            raise AttributeError( 'accessing %r' % name )

    def __setattr__( self, name, value ) :
        """Not allowed"""
        if name.startswith('_x_') :
            self.__dict__[name] = value
        else :
            raise Exception( 'Immutable document !!' )
        return value

    def __getitem__( self, name ):
        return dict.__getitem__( self, name )

    def __setitem__(self, key, value) :
        """Not allowed"""
        raise Exception( 'Immutable document !!' )

    def __delitem__( self, key ) :
        """Not allowed"""
        raise Exception( 'Immutable document !!' )

    def clear( self ):
        """Not allowed"""
        raise Exception( 'Immutable document !!' )

    def update( self, *args, **kwargs ):
        """Not allowed"""
        raise Exception( 'Immutable document !!' )

    def setdefault( self, key, *args ):
        """Not allowed"""
        raise Exception( 'Immutable document !!' )

    def pop( self, key, *args ):
        """Not allowed"""
        raise Exception( 'Immutable document !!' )

    def popitem( self ):
        """Not allowed"""
        raise Exception( 'Immutable document !!' )

    def __repr__( self ):
        _id = self.get('_id', None)
        _rev = self.get('_rev', None)
        return '<%s %r:%r>' % (type(self).__name__, _id, _rev)

    def fetch( self, hthdrs={}, **query ):
        """GET this document from disk. Always fetch the latest revision of
        the document.

        Optional keyword parameters,

        ``hthdrs``,
            HTTP headers for this HTTP request.

        ``revs``,
            Get the document with a list of all revisions on the disk.

        ``revs_info``,
            Get the document with a list of extended revision infor.

        Returns this document object.

        Admin-prev, No
        """
        conn, paths = self._x_conn, self._x_paths
        q = {}
        q.update( self._x_query, rev=self['_rev'] )
        q.update( query )
        s, h, doc = _getdoc( conn, paths, hthdrs=self._x_hthdrs, **q )
        if doc :
            dict.clear( self )
            dict.update( self, doc )
        return self



#---- Design documents APIs

def _infosgn( conn, paths=[], hthdrs={} ) :
    """GET /<db>/_design/<design-doc>/_info"""
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs )
    s, h, d = conn.delete( paths, hthdrs, None )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)


class DesignDocument( Document ):
    """Derived from :class:`couchpy.doc.Document` class, encapsulates a design
    document. Initialization, creation and other operations are
    exactly similar to that of normal documents, except for the following
    additional details.
    """

    def __init__( self, *args, **kwargs ):
        Document.__init__( self, *args, **kwargs )
        # Fix the _x_paths
        if '_design' not in self._x_paths[-1] :
            self._x_paths.insert( -1, '_design' )

    def info( self ):
        conn, paths = self._x_conn, self._x_paths
        hthdrs = conn.mixinhdrs( self._x_hthdrs, hthdrs )
        s, h, d = _infosgn( conn, paths, hthdrs=hthdrs, **query )
        return d

    def views( self ):
        return Views( self, self['views'], hthdrs=self._x_hthdrs )


class Views( dict ) :
    """Dictionary of views object for design documents."""

    def __init__( self, doc, views, *args, **kwargs ):
        self._x_hthdrs = kwargs.pop( 'hthdrs', {} )
        self._x_doc    = doc
        views_ = dict([
            ( viewname,
              View( self._x_doc, viewname, viewdict, hthdrs=self._x_hthdrs )
            ) for viewname, viewdict in views.items()
        ])
        dict.__init__( self, views )

    def __getattr__( self, name ) :
        if name in self :
            return self[name]
        else :
            raise AttributeError( 'accessing %r' % name )

    def __setattr__( self, name, value ) :
        """Not allowed"""
        if name.startswith('_x_') :
            return setattr( self, name, value )
        else :
            raise Exception( 'Immutable object !!' )


def _viewsgn( conn, keys=None, paths=[], hthdrs={}, q={} ) :
    """
    GET  /<db>/_design/<design-doc>/_view/<view-name>,
    POST /<db>/_design/<design-doc>/_view/<view-name>,

    query object `q` for GET,
        descending=<bool>   endkey=<key>        endkey_docid=<id>
        group=<bool>        group_level=<num>   include_docs=<bool>
        key=<key>           limit=<num>         inclusive_end=<bool>
        reduce=<bool>       skip=<num>          stale='ok'
        startkey=<key>      startkey_docid=<id> update_seq=<bool>
    Note that `q` object should provide .items() method with will return a
    list of key,value query parameters.
    """
    hthdrs = conn.mixinhdrs( hthdrs, hdr_acceptjs, hdr_ctypejs )
    if keys :
        body = rest.data2json({ 'keys' : keys })
        s, h, d = conn.post( paths, hthdrs, body, _query=q_.items() )
    else :
        s, h, d = conn.get( paths, hthdrs, None, _query=q_.items() )

    if s == OK :
        return s, h, d
    else :
        return (None, None, None)


class View( object ) :

    def __init__( self, doc, viewname, view, hthdrs={}, q={} ):
        self.doc, self.viewname, self.view = doc, viewname, view
        self.conn     = doc._x_conn
        seld.paths    = doc._x_paths + [ '_view', viewname ]
        self.hthdrs   = doc._x_conn.mixinhdrs( doc._x_hthdrs, hthdrs )
        self.query    = Query( q=q )

    def __call__( self, hthdrs={}, q={}, **params ):
        query = Query( q=self.query )
        query.update( q )
        query.update( param )
        return View( self.doc, self.viewname, self.view, query=query )

    def fetch( self, keys=None, hthdrs={}, query={}, **params ):
        """View query.
        If ``keys`` is None, then all the documents in the view index will be
        fetched, using ``query`` parameter if provided or using the default
        query object provided during View instantiation.

        Admin-prev, No
        """
        query = Query( self.query )
        query.update( query )
        query.update( params )
        conn, paths = self.conn, self.paths
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _viewsgn(conn, keys=keys, paths=paths, hthdrs=hthdrs, q=query)
        return d


class Query( dict ) :
    def __init__( self, q={}, **params ) :
        """Create a Query object using, keyword arguments which map to the
        view query parameters. Optionally, a dictionary of query parameters
        can be passed via the keyword argument ``q``.
        """
        self.update( q )
        self.update( params )
        self._jsonifyall()

    def __getattr__( self, name ) :
        """Access views attributes of this instance."""
        if name in self :
            return self[name]
        else :
            raise AttributeError( 'accessing %r' % name )

    def __setattr__( self, name, value ) :
        """Set query parameters as attributes to this instance"""
        self[name] = value

    def __getitem__( self, name ):
        return dict.__getitem__( self, name )

    def __setitem__(self, key, value) :
        return dict.__setitem__( key, self._jsonify( key, value ))

    def __delitem__( self, key ) :
        return dict.__delitem__( self, key )

    def clear( self ):
        return dict.clear( self )

    def update( self, *args, **kwargs ):
        d = {}
        [ d.update(arg) for arg in args ]
        d.update( kwargs )
        d = dict([ (k, self._jsonify(k,v)) for k,v in d.items() ])
        return dict.update( self, d )

    def setdefault( self, key, *args ):
        value = dict.setdefault( self, key, *args )
        return self._jsonify( key, value )

    def pop( self, key, *args ):
        return dict.pop( key, *args )

    def popitem( self ):
        return dict.popitem()

    def __call__( self, q={}, **params ) :
        """Factory method to create new query objects overriding current parameter
        list, with keyword arguments. Optionally, a dictionary of query parameters
        can be passed via the keyword argument ``q``.
        """
        d = dict( self.items() )
        d.update( q )
        d.update( params )
        return Query( q=d )

    def __repr__( self ) : 
        """Construct url query string from key,value pairs and return the same.
        """
        return "'%s'" % self._str__

    def __str__( self ) : 
        """Construct url query string from key,value pairs and return the same.
        """
        r = '&'.join([ '%s=%s' % (k,v) for k,v in self._params.items() ])
        return '?%s' % r if r else ''

    def _jsonifyall( self ):
        startkey_docid = self.get( 'startkey_docid', None )
        endkey_docid   = self.get( 'endkey_docid', None )
        self.update( dict([ (k, rest.data2json(v)) for k, v in self.items() ]))
        if startkey_docid :
            self['startkey_docid'] = startkey_docid
        if endkey_docid :
            self['endkey_docid'] = endkey_docid

    def _jsonify( self, name, value ):
        if name in [ 'startkey_docid', 'endkey_docid' ] :
            return value
        else :
            return rest.data2json(value)


#---- Document, Design document structres

"""General document structure,
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
    '_revisions' : {
        ids : [ ... ],
        start : <num>
    },
    '_revs_info' : [
        { 'rev' : <full-rev>, 'status' : <string> },
        ...
    ],
    '_deleted' : true,
    '_conflict' : true,
}
"""

"""design document specific structure,
{
    "language" : <viewserver-language>,

    "views" : {
        <viewname> : {
            "map"    : "function( doc ) { ... };",
            "reduce" : "function( keys, values, rereduce ) { ... };",
        },
        ...
    },

    "validate_doc_update" : "function( newDoc, oldDoc, userCtx ) { ... };",

    "shows" : {
        <showname> : "function( doc, req ) { ... };",
        ...
    },
}
"""
