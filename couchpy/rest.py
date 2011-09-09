#!/usr/bin/env python
# -*- coding: utf-8 -*-

# This file is subject to the terms and conditions defined in
# file 'LICENSE', which is part of this source code package.
#       Copyright (c) 2010 SKR Farms (P) LTD.

# TODO :
#   1. While json.loads(), provision for max_depth can help in optimization.
#   2. Instead of using python based json, it would be great to use c-based
#      implementation

import urllib, json, logging, sys, time
from   copy             import deepcopy
from   urlparse         import urlsplit, urlunsplit
from   StringIO         import StringIO

from   couchpy.mixins   import Helpers
import httpc

log = logging.getLogger( __name__ )

class ReSTful( Helpers, object ) :
    """
    Class definition along with the HttpSession to interface a HTTP server
    using ReST-ful (Representational State Transfer) design. If http-header
    `Accept` has only `application/json` as the value, then reponse data will
    be converted from json to python object.

    url, 
        base url to be used to compose the full resource-url
    htsess,
        HttpSession object for clientside connection. If not supplied, a
        new instance will be created and remembered until the life of this
        instance.
    headers,
        dictionary of http-headers that will be used for all http-request
        made my this object.
    """

    def __init__( self, url, htsess, headers=None ) :
        self.url, self.credentials = _extract_credentials(url)
        self.htsession = httpc.HttpSession() if htsess is None else htsess
        self.headers = headers or {}
        self.is_jsonresp = headers.get('Accept', '') == 'application/json'

    def __call__( self, *path ) :
        """
        Return a clone of this object, with more specific url path-info.
        """
        obj = type(self)( urljoin(self.url, *path), self.htsession )
        obj.credentials = deepcopy( self.credentials )
        obj.headers = deepcopy( self.headers )
        return obj

    def _jsonloads( self, hdr, data ) :
        if 'application/json' in hdr.get( 'content-type', '' ) :
            data = json.loads( data.getvalue() )
        return data

    def head( self, paths, hdrs, body, _query=[] ) :
        """
        HEAD request with hdrs and body, for resource specified by 
        base-url (provided while instantiation) and path. Optional _query,
        which is list of key,value tuples to construct url-query, http headers
        along with request content.

        Returns,
            status, headers, data
        """
        s, h, d = self._request('HEAD', paths, hdrs, body, _query)
        d = self._jsonloads( h, d ) if d != None else d
        return s, h, d

    def get( self, paths, hdrs, body, _query=[] ) :
        """
        GET request with hdrs and body, for resource specified by 
        base-url (provided while instantiation) and path. Optional _query,
        which is list of key,value tuples to construct url-query, http headers
        along with request content.

        Returns,
            status, headers, data
        """
        s, h, d = self._request('GET', paths, hdrs, body, _query)
        d = self._jsonloads( h, d )
        return s, h, d

    def post( self, paths, hdrs, body, _query=[] ) :
        """
        POST request with hdrs and body, for resource specified by 
        base-url (provided while instantiation) and path. Optional _query,
        which is list of key,value tuples to construct url-query, http headers
        along with request content.

        Returns,
            status, headers, data
        """
        s, h, d = self._request('POST', paths, hdrs, body, _query)
        d = self._jsonloads( h, d )
        return s, h, d


    def put( self, paths, hdrs, body, _query=[] ) :
        """
        PUT request with hdrs and body, for resource specified by 
        base-url (provided while instantiation) and path. Optional _query,
        which is list of key,value tuples to construct url-query, http headers
        along with request content.

        Returns,
            status, headers, data
        """
        s, h, d = self._request('PUT', paths, hdrs, body, _query)
        d = self._jsonloads( h, d )
        return s, h, d

    def delete( self, paths, hdrs, body, _query=[] ) :
        """
        DELETE request with hdrs and body, for resource specified by 
        base-url (provided while instantiation) and path. Optional _query,
        which is list of key,value tuples to construct url-query, http headers
        along with request content.

        Returns,
            status, headers, data
        """
        s, h, d = self._request('DELETE', paths, hdrs, body, _query)
        d = self._jsonloads( h, d )
        return s, h, d

    def copy( self, paths, hdrs, body, _query=[] ) :
        """
        COPY request with hdrs and body, for resource specified by 
        base-url (provided while instantiation) and path. Optional _query,
        which is list of key,value tuples to construct url-query, http headers
        along with request content.

        Returns,
            status, headers, data
        """
        s, h, d = self._request('COPY', paths, hdrs, body, _query)
        d = self._jsonloads( h, d )
        return s, h, d

    def _request( self, method, paths, headers, body, _query ):
        all_headers = deepcopy( self.headers )
        all_headers.update( headers or {} )
        paths = paths.split('/') if isinstance( paths, basestring ) else paths
        paths = filter( None, paths )
        url = urljoin( self.url, *paths, _query=_query )
        st = time.time()    # Debog code
        resp = self.htsession.request(
                    method, url, body=body, headers=all_headers,
                    credentials=self.credentials
               )
        log.info( "%6s %s %s (%s)" % (method, (time.time()-st), url, resp[0]) )
        return resp


def urljoin( base, *path, **kwargs ) :
    """Assemble a uri based on a base, any number of path segments, and
    query key-word argument. query, is a list of key,value tuples.

    >>> urljoin('http://example.org', '_all_dbs')
    'http://example.org/_all_dbs'

    And multiple positional arguments become path parts:

    >>> urljoin('http://example.org/', 'foo', 'bar')
    'http://example.org/foo/bar'

    All slashes within a path part are escaped:

    >>> urljoin('http://example.org/', 'foo/bar')
    'http://example.org/foo%2Fbar'
    >>> urljoin('http://example.org/', 'foo', '/bar/')
    'http://example.org/foo/%2Fbar%2F'
    """
    base = base[:-1] if base and base.endswith('/') else base
    _query = kwargs.get( '_query', [] )

    # build the path
    path = '/'.join([ _quote(s) for s in path if s ])
    url = '/'.join([ base, path ])

    # build the query string
    params = []
    for name, value in _query :
        value = value if type(value) in (list, tuple) else [value]
        fn = lambda x : ( name, _normalize(x) )
        params.extend( map( lambda x : (name, x), filter(None, value) ))
    url = '?'.join([ url, _urlencode(params) ]) if params else url
    return url

def _extract_credentials( url ) :
    """Extract authentication (user name and password) credentials from the
    given URL.
    
    >>> _extract_credentials('http://localhost:5984/_config/')
    ('http://localhost:5984/_config/', None)
    >>> _extract_credentials('http://joe:secret@localhost:5984/_config/')
    ('http://localhost:5984/_config/', ('joe', 'secret'))
    >>> url ='http://joe%40example.com:secret@localhost:5984/_config/'
    >>> _extract_credentials(url)
    ('http://localhost:5984/_config/', ('joe@example.com', 'secret'))
    """
    parts = urlsplit(url)
    netloc = parts[1]
    credentials = None
    if '@' in netloc :
        creds, netloc = netloc.split('@')
        credentials = tuple(urllib.unquote(i) for i in creds.split(':'))
        parts = list(parts)
        parts[1] = netloc
    return urlunsplit(parts), credentials

def _quote( s, safe='' ) :
    s = s.encode('utf-8') if isinstance( s, unicode ) else s
    return urllib.quote(s, safe)

def _urlencode( data ) :
    data = data.items() if isinstance(data, dict) else data
    fn = lambda v : v.encode('utf-8') if isinstance(v, unicode) else v
    params = [ (name, fn(value)) for name, value in data ]
    query = '&'.join([ '%s=%s'%(k,v) for k, v in  params ])
    return urllib.quote(query, '&=\'"')

def _normalize( val ) :
    if val == True : return 'true'
    if val == False : return 'false'

def data2json( data ) :
    buf = StringIO()
    json.dump({}, buf) if data == None else json.dump(data, buf)
    x = buf.getvalue()
    return x
