"""Some times many entries in a view have the same key value. To differentiate
among such entries use doc-id. Note key and doc-id will be different in such
cases and don't expect doc-id to be in sort order
"""

from    copy                    import deepcopy

from    couchpy.designdoc       import DesignDocument
from    couchpy.query           import Query
from    couchpy.view            import View

class Paginate( object ) :
    """Pagination object to fetch database entries as pages, uses CouchDB
    view-query parameters to implement pagination, though only a subset of the
    entire parameter list is interepreted. They are,
    
    ``limit``
        Number of entries per page.
    ``startkey``
        Starting value of the key in the page's list
    ``endkey``
        Ending value of the key in the page's list
    ``startkey_docid``
        Starting doc-id in the page's list. When key & id are same (like when
        using _all_docs API) the doc-id will be sorted as well. Otherwise only
        expect the ``key`` value to be sorted.
    ``endkey_docid``
        Ending doc-id in the page's list. When key & id are same (like when
        using _all_docs API) the doc-id will be sorted as well. Otherwise only
        expect the ``key`` value to be sorted.
    ``inclusive_end``
        Specifies whether the specified end key should be included in the 
        result.
    ``skip``
        Number of entries to skip from starting.
    """

    LIMIT = 10
    querykeys = [
        'limit', 'descending', 'startkey', 'endkey', 'startkey_docid',
        'endkey_docid', 'inclusive_end', 'skip'
    ]

    def __init__( self, db, ddoc, viewname, cookie=None,
                  hthdrs={}, _q={}, **query ) :
        self.usedocid = query.pop( 'usedocid', False )
        self.query = deepcopy(_q)
        self.query.update(query)
        self.query.setdefault( 'limit', self.LIMIT )
        self.db = db
        self.ddoc = ddoc
        self.viewname = viewname
        self.hthdrs = hthdrs
        self.cookie = self.cookieload( cookie )
        self.startkey = self.cookie.get( 'startkey', None )
        self.endkey = self.cookie.get( 'endkey', None )
        if self.usedocid :
            self.startkey_docid = self.cookie.get( 'startkey_docid', None )
            self.endkey_docid = self.cookie.get( 'endkey_docid', None )

    def _view( self ) :
        """View object"""
        def fndesign( _q={} ) :
            return View( self.db, self.ddoc, self.viewname, hthdrs=self.hthdrs
                   )( _q=_q )
        def fn( _q={} ) :
            return self.db.docs( hthdrs=self.hthdrs, _q=_q )
        if self.ddoc and self.viewname :
            return fndesign
        else :
            return fn

    def _query( self, **q ) :
        """Query object based on the current state of the page and requested
        command (like, next, prev, etc ...)"""
        q_ = deepcopy( self.query )
        q_.update(q)
        strkeys = [ 'startkey', 'startkey_docid', 'endkey', 'endkey_docid' ]
        [ q_.update({ k : '"%s"'%q_[k] })   for k in strkeys if k in q_ ]
        return q_

    def _prune( self, result, key, limit ) :
        if result['rows'] and (result['rows'][0]['key'] == key) :
            result['rows'] = result['rows'][1:]
        else :
            result['rows'] = result['rows'][:limit]
        return result

    def _updatestartkeys( self, entry ) :
        self.startkey, self.startkey_docid = entry['key'], entry['id']

    def _updateendkeys( self, entry ) :
        self.endkey, self.endkey_docid = entry['key'], entry['id']

    def _updatekeys( self, forstart, forend ) :
        self._updatestartkeys( forstart )
        self._updateendkeys( forend )

    def page( self, limit=None, sticky=True ) :
        """Fetch a list of entries for the current page based on the initial
        query parameters
        """
        result = self._view()( _q=self._query() )
        rows = result['rows']
        self.startkey, self.startkey_docid = rows[0]['key'], rows[0]['id']
        self.endkey, self.endkey_docid = rows[-1]['key'], rows[-1]['id']
        return result

    def next( self, limit=None, sticky=True ) :
        """Fetch a list of entries for the next page (in reference to this
        page) based on the remembered query parameter. By default the new
        page window is remembered, to avoid that pass key-word argument ``sticky``
        as False
        """
        result = {}
        if self.endkey :
            limit = (limit or self.query['limit']) + 1
            q = { 'startkey' : self.endkey, 'limit' : limit }
            if self.usedocid :
                q['startkey_docid'] = self.endkey_docid 
            result = self._view()( _q=self._query(**q) )
            result = self._prune( result, self.endkey, limit )
            rows = result['rows']
            self._updatekeys(rows[0], rows[-1]) if sticky and len(rows) else None
        return result

    def prev( self, limit=None, sticky=True ) :
        """Fetch a list of entries for the previous page (in reference to this
        page) based on the remembered query parameter. By default the new
        page window is remembered, to avoid that pass key-word argument ``sticky``
        as False
        """
        result = {}
        if self.startkey :
            limit = (limit or self.query['limit']) + 1
            descending = self.query.get( 'descending', False )
            q = { 'startkey' : self.startkey, 'limit' : limit,
                  'descending' : not descending, }
            if self.usedocid :
                q['startkey_docid'] = self.startkey_docid
            result = self._view()( _q=self._query(**q) )
            result = self._prune( result, self.startkey, limit )
            rows = result['rows']
            rows.reverse()
            self._updatekeys(rows[0], rows[-1]) if sticky and len(rows) else None
        return result

    def fewmore( self, limit=1, sticky=True ) :
        """Fetch few more entries, specified by ``limit`` after this page.
        Instead of remembering it as a new page window, simply extend the
        current page window by few more entries. Again this can be disabled by
        passing key-word argument ``sticky`` as False.
        """
        result = {}
        if self.endkey :
            q = { 'startkey' : self.endkey, 'limit' : limit+1 }
            if self.usedocid :
                q['startkey_docid'] = self.endkey_docid 
            result = self._view()( _q=self._query(**q) )
            result = self._prune( result, self.endkey, limit )
            rows = result['rows']
            self._updateendkeys(rows[-1]) if sticky and len(rows) else None
        return result

    def remember( self ) :
        """Return a python evaluatable string, representing a dictionary of
        current query values"""
        q = deepcopy( self.query )
        q.setdefault('startkey', self.startkey) if self.startkey else None
        q.setdefault('endkey', self.endkey) if self.endkey else None
        if self.usedocid :
            q.setdefault('startkey_docid', self.startkey_docid)
            q.setdefault('endkey_docid', self.endkey_docid)
        return str(dict(filter(lambda x : x[1]!=None, q.items() )))

    def cookieload( self, s ) :
        """Load a saved query in cookie, so that pagination can work relative
        to user's view"""
        try :
            query = eval( s )
        except :
            query = {}
        return query
