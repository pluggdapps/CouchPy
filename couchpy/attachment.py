"""Module provides provides a convinient class :class:`Attachment` to access (Create,
Read, Delete) document attachments."""

from   copy             import deepcopy
from   mimetypes        import guess_type
import base64

from   httperror        import *
from   httpc            import HttpSession, ResourceNotFound, OK, CREATED
from   couchpy          import CouchPyError

# TODO :
#   1. URL-encoding for attachment file-names

def _readattach( conn, paths=[], hthdrs={} ) :
    """
    GET /<db>/<doc>/<attachment>
    GET /<db>/_design/<design-doc>/<attachment>
    """
    s, h, d = conn.get( paths, hthdrs, None )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

def _writeattach( conn, paths=[], body='', hthdrs={}, **query ) :
    """
    PUT /<db>/<doc>/<attachment>
    PUT /<db>/_design/<design-doc>/<attachment>
    query,
        rev=<_rev>
    """
    if 'Content-Length' not in hthdrs :
        raise CouchPyError( '`Content-Length` header field not supplied' )
    if 'Content-Type' not in hthdrs :
        raise CouchPyError( '`Content-Type` header field not supplied' )
    s, h, d = conn.put( paths, hthdrs, body, _query=query.items() )
    if s == OK and d['ok'] == True :
        return s, h, d
    else :
        return (None, None, None)

def _deleteattach( conn, paths=[], hthdrs={}, **query ) :
    """
    DELETE /<db>/<doc>/<attachment>
    DELETE /<db>/_design/<design-doc>/<attachment>
    query,
        rev=<_rev>
    """
    s, h, d = conn.delete( paths, hthdrs, None, _query=query.items() )
    if s == OK and d['ok'] == True :
        return s, h, d
    else :
        return (None, None, None)

class Attachment( object ) :
    def __init__( self, doc, filename ) :
        """Class instance object represents a single attachment in a document,
        use the :class:`Document` object and attachment `filename` to create
        the instance.
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
        """Returns the content of the file attached to the document. Can
        optionally take a dictionary of http headers.
        """
        data, content_type = self.getattachment( 
                                self.db, self.doc, self.filename, hthdrs=hthdrs
                             )
        return data, content_type

    content_type = property( lambda self : self.attachinfo('content_type') )
    length = property( lambda self : self.attachinfo('length') )
    revpos = property( lambda self : self.attachinfo('revpos') )
    stub = property( lambda self : self.attachinfo('stub') )
    content = property( lambda self : self.data() )
        
    @classmethod
    def getattachment( cls, db, doc, filename, hthdrs={} ) :
        """Returns a tuple of, ( <filedata>, <content_type> )
        for attachment `filename` in `doc` stored in database `db`
        """
        id_ = doc if isinstance(doc, basestring) else doc._id
        paths = db.paths + [ id_, filename ]
        s, h, d = _readattach( db.conn, paths, hthdrs=hthdrs )
        content_type = h.get( 'Content-Type', None )
        return (d, content_type)

    @classmethod
    def putattachment( cls, db, doc, filename, data, content_type=None,
                       hthdrs={}, **query ) :
        """Upload the supplied content (data) as attachment to the specified
        document (doc). `filename` provided must be a URL encoded string.
        If `doc` is document-id, then `rev` keyword parameter should be
        present in query.
        """
        id_ = doc if isinstance(doc, basestring) else doc._id
        rev = query['rev'] if 'rev' in query else doc._rev
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
    def delattachment( cls, db, doc, filename, hthdrs={}, **query ) :
        """Deletes the attachment form the specified doc. You must
        supply the rev argument with the current revision to delete the
        attachment."""
        id_ = doc if isinstance(doc, basestring) else doc._id
        rev = query['rev'] if 'rev' in query else doc._rev
        paths = db.paths + [ id_, filename ]
        s, h, d = _deleteattach( db.conn, paths, hthdrs=hthdrs, rev=rev )
        if isinstance(doc, Document) and d != None :
            doc.update({ '_rev' : d['rev'] })
        return d

    @classmethod
    def files2attach( cls, fnames=[] ) :
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

