"""View class to contruct, fetch CoucDB database views."""

import logging, time
from   copy                 import deepcopy

import couchpy.rest         as rest
from   couchpy.query        import Query
from   couchpy.designdoc    import DesignDocument
from   couchpy.httpc        import HttpSession, ResourceNotFound, OK, CREATED

log = logging.getLogger( __name__ )
hdr_acceptjs = { 'Accept' : 'application/json' }
hdr_ctypejs  = { 'Content-Type' : 'application/json' }

def _viewsgn( conn, keys=None, paths=[], hthdrs={}, q={} ) :
    """
    GET  /<db>/_design/<design-doc>/_view/<view-name>,
         if keys is None
    POST /<db>/_design/<design-doc>/_view/<view-name>,
         if keys is a list of view keys to select
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
    # Convert query into JSON-utf8
    q_ = dict([ ( k, rest.data2json(v) ) for k, v in q.items() ])
    if 'startkey_docid' in q :
        q_['startkey_docid'] = q['startkey_docid']
    if 'endkey_docid' in q :
        q_['endkey_docid'] = q['endkey_docid']
    #---
    if keys == None :
        s, h, d = conn.get( paths, hthdrs, None, _query=q_.items() )
    else :
        body = rest.data2json({ 'keys' : keys })
        s, h, d = conn.post( paths, hthdrs, body, _query=q_.items() )
    if s == OK :
        return s, h, d
    else :
        return (None, None, None)

class View( object ) :
    def __init__( self, db, designdoc, viewname, hthdrs={}, _q={}, **query ) :
        """Instantiate a view from database base ``db`` under ``designdoc``.
        ``viewname`` should be the name of the view as defined by the designdoc.
        Optionally pass the ``_q`` Query object (or dictionary of query params)
        to initialize the default query. query-parameters can also be passed
        in as key-word arguments

        Query parameters for composite keys must be passed as json convertible
        python objects.
        """
        self.db = db
        self.conn = db.conn
        self.client = db.client
        self.debug = db.debug
        if isinstance(designdoc, basestring) :
            p = db.paths + designdoc.split('/')
        elif isinstance(designdoc, list) :
            p = db.paths + designdoc
        elif isinstance(designdoc, DesignDocument) :
            p = designdoc.paths
        self.paths = p + ['_view', viewname]
        q = _q if isinstance(_q, Query) else Query( params=_q )
        q.update( query )
        self.hthdrs = self.conn.mixinhdrs( db.hthdrs, hthdrs )

    def __call__( self, keys=None, hthdrs={}, _q=None, **query ) :
        """Execute the view using default query or using query object `_q` and
        key-word parameters `query`

        Query parameters for composite keys must be passed as json convertible
        python objects.

        Return,
            JSON converted object as returned by CouchdB
        Admin-prev,
            No
        """
        q = deepcopy( self.q if _q == None else _q )
        q.update( query )
        conn, paths = self.conn, self.paths
        hthdrs = conn.mixinhdrs( self.hthdrs, hthdrs )
        s, h, d = _viewsgn( conn, keys=keys, paths=paths, hthdrs=hthdrs, q=q )
        return d
