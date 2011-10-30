# This file is subject to the terms and conditions defined in
# file 'LICENSE', which is part of this source code package.
#       Copyright (c) 2011 SKR Farms (P) LTD.

# -*- coding: utf-8 -*-

"""Some times many entries in a view have the same key value. To differentiate
among such entries use doc-id. Note key and doc-id will be different in such
cases and don't expect doc-id to be in sort order
"""

from copy            import deepcopy
from couchpy.doc     import View

# Gotcha :
#   1. There is a big gotcha that seems to be present in Python. That is, when
#   we initialize a key-word argument with empty list literal like '[]', and
#   inside the function populate the list with content, which subsequently
#   gets passed all over. Somehow, when the function is called the second
#   time, the empty list literal 're-uses' the same list object again. Not
#   sure whether this is a documented feature. Or whether I am missing
#   something here.

class Paginate( object ) :
    """Pagination object to fetch database entries as pages, uses CouchDB
    view-query parameters to implement pagination, though only a subset of the
    entire parameter list is interepreted. They are,
    
    ``limit``
        Number of entries per page.
    ``key``
        Documents that match the specified key.
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
    Other view-query parameters are passes as is to the database view.

    Additionaly, key-word argument `usedocid` can be used to setup pagination
    based on document-id (along with its key value). To enable this pass
    `usedocid` as `true`
    """

    LIMIT = 10
    querykeys = [
        'limit', 'descending', 'startkey', 'endkey', 'startkey_docid',
        'endkey_docid', 'inclusive_end', 'skip'
    ]

    def __init__( self, db, ddoc, viewname, cookie=None,
                  hthdrs={}, _q={}, **query ) :
        self.usedocid = query.pop( 'usedocid', False )
        # Database view parmaters
        self.db = db
        self.ddoc = ddoc
        self.viewname = viewname
        self.hthdrs = hthdrs
        # Merge view query parameters from the argument list
        self.query = deepcopy(_q)
        self.query.update(query)
        self.query.setdefault( 'limit', self.LIMIT )
        # Pagination is to be initialized based on current status of the page.
        self.cookie = self.cookieload( cookie )
        cookeys = ['key', 'startkey', 'endkey', 'startkey_docid', 'endkey_docid']
        [ setattr( self, k, self.cookie.get(k, None) ) for k in cookeys ]
        

    def _view( self ) :
        """View object, this interface automatically switches to _all_docs API
        if design document / viewname is not specified"""
        def fndesign( _q={} ) :     # Use design document view API
            v = View( self.db, self.ddoc, self.viewname, hthdrs=self.hthdrs)
            result = v( _q=_q )
            rows = result.get( 'rows', [] )
            return rows

        def fn( _q={} ) :           # Use _all_docs API
            result = self.db.docs( hthdrs=self.hthdrs, _q=_q )
            rows = result.get( 'rows', [] )
            return rows

        return fndesign if self.ddoc and self.viewname else fn

    def _query( self, **q ) :
        """Query object based on the current state of the page and requested
        command (like, next, prev, etc ...)"""
        q_ = deepcopy( self.query )
        q_.update(q)
        return q_

    def _marker( self, q, _key=None, _id=None ) :
        if _key or _id :
            if _key : q['startkey'] = _key
            if _id and self.usedocid : q['startkey_docid'] = _id
        return q

    def _checkmarker( self, row, marker ) :
        if row['key'] != marker['startkey'] : return False
        if self.usedocid and (row['id'] != marker['startkey_docid']) : return False
        return True

    def _prunestarting( self, rows, _key=None, _id=None ) :
        if self.usedocid and _id and (rows[0]['id'] == _id) :
            rows = rows[1:]
        elif _key :
            while rows and (rows[0]['key'] == _key) : rows.pop(0)
        return rows

    def _pruneending( self, rows, _key=None, _id=None ) :
        if self.usedocid and _id and (rows[-1]['id'] == _id) :
            rows = rows[:-1]
        if _key :
            while rows and rows[-1]['key'] == _key : rows.pop(-1)
        return rows

    def _updatestartkeys( self, entry ) :
        self.startkey, self.startkey_docid = entry['key'], entry['id']

    def _updateendkeys( self, entry ) :
        self.endkey, self.endkey_docid = entry['key'], entry['id']

    def _updatekeys( self, forstart, forend ) :
        self._updatestartkeys( forstart )
        self._updateendkeys( forend )

    def page( self, sticky=True, **q ) :
        """Fetch a list of entries for the current page based on the initial
        query parameters
        """
        # Fetch one more than what is need to know whether there are more
        # entries available for fetching.
        limit = q.get( 'limit', self.query['limit'] )
        q['limit'] = limit + 1
        rows = self._view()( _q=self._query(**q) )
        # Process the result
        rowlen, next_ = len(rows), False
        if rowlen > limit :
            rows = rows[:-1]
            next_ = True 
        # Update keys
        (sticky and rowlen) and self._updatekeys(rows[0], rows[-1]) 
        return (False, rows, next_)

    def next( self, sticky=True, **q ) :
        """Fetch a list of entries for the next page (in reference to this
        page) based on the remembered query parameter. By default the new
        page window is remembered, to avoid that pass key-word argument
        ``sticky`` as False
        """
        # Fetch one more than what is need to know whether there are more
        # entries available for fetching.
        limit = q.get( 'limit', self.query['limit'] )
        q['limit'] = limit + 2
        m = self._marker( {}, self.endkey, self.endkey_docid )
        q.update(m)
        rows = self._view()( _q=self._query(**q) )
        # Process result
        next_, prev_ = False, False
        if len(rows) and self._checkmarker( rows[0], m ) :
            _key, _id = rows[0]['key'], rows[0]['id']
            rows = self._prunestarting( rows, _key, _id )
            prev_ = True
        if len(rows) > limit :
            rows = rows[:(limit-len(rows))]
            next_ = True
        # Update keys
        (sticky and len(rows)) and self._updatekeys( rows[0], rows[-1] )
        return prev_, rows, next_

    def fewmore( self, sticky=True, **q ) :
        """Fetch few more entries, specified by ``limit`` after this page.
        Instead of remembering it as a new page window, simply extend the
        current page window by few more entries. Again this can be disabled by
        passing key-word argument ``sticky`` as False.
        """
        # Fetch one more than what is need to know whether there are more
        # entries available for fetching.
        limit = q.get( 'limit', self.query['limit'] )
        q['limit'] = limit + 2
        m = self._marker( {}, self.endkey, self.endkey_docid )
        q.update(m)
        rows = self._view()( _q=self._query(**q) )
        # Process result
        next_, prev_ = False, False
        if len(rows) and self._checkmarker( rows[0], m ) :
            _key, _id = rows[0]['key'], rows[0]['id']
            rows = self._prunestarting( rows, _key, _id )
            prev_ = True
        if len(rows) > limit :
            rows = rows[:(limit-len(rows))]
            next_ = True
        # Update keys
        (sticky and len(rows)) and self._updateendkeys(rows[-1])
        return (prev_, rows, next_)

    def prev( self, sticky=True, **q ) :
        """Fetch a list of entries for the previous page (in reference to this
        page) based on the remembered query parameter. By default the new
        page window is remembered, to avoid that pass key-word argument ``sticky``
        as False
        """
        # Fetch one more than what is need to know whether there are more
        # entries available for fetching.
        limit = q.get( 'limit', self.query['limit'] )
        q.update({ 'limit' : limit+2, 'descending' : True })
        m = self._marker( {}, self.startkey, self.startkey_docid )
        q.update( m )
        rows = self._view()( _q=self._query(**q) )
        # Process result
        next_, prev_ = False, False
        if len(rows) and self._checkmarker( rows[0], m ) :
            _key, _id = rows[0]['key'], rows[0]['id']
            rows = self._prunestarting( rows, _key, _id )
            next_ = True
        if len(rows) > limit :
            rows = rows[:(limit-len(rows))]
            prev_ = True
        # Update keys
        rows.reverse()
        (sticky and len(rows)) and self._updatekeys(rows[0], rows[-1]) 
        return prev_, rows, next_

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
