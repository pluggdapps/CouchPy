# CouchPy Couchdb data-modeling for CouchDB database management systems
#   Copyright (C) 2011  SKR Farms (P) LTD
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-

import urllib, logging, time
from   copy             import deepcopy
from   urlparse         import urlsplit, urlunsplit

from   couchpy.json     import JSON

log = logging.getLogger( __name__ )

class ReSTful( object ) :
    """ReST-ful (Representational State Transfer) interface to CouchDB server.
    Uses a httpclient session and provides method APIs for rest of the
    couchpy modules. If http-header ``Accept`` has only ``application/json`` as
    the value, then reponse data will be converted from json to python
    object.

    Constructor arugments,

    ``url``, 
        base url to be used to compose the full resource-url. url can contain
        credential information as <username>:<password>
    ``htsess``,
        HttpSession object for clientside connection. If not supplied, a
        new instance will be created and remembered until this instance gets
        garbage-collected.
    ``headers``,
        dictionary of http-headers that will be used for all http-request
        made my this object. The header fields supplied here are overridable
        in method APIs.
    """

    def __init__( self, url, htsess, headers=None ) :
        self.url, self.credentials = _extract_credentials(url)
        self.htsess = htsess or self._httpsession()
        self.headers = headers or {}
        self.is_jsonresp = headers.get('Accept', '') == 'application/json'

    def __call__( self, *path, **kwargs ):
        """Return a clone of this object, with more specific url path-info, an
        optional http-session ``htsess`` and an optional set of ``headers``.
        """
        obj = type(self)( urljoin(self.url, *path),
                          kwargs.get( 'htsess', self.htsess ),
                          kwargs.get( 'headers', deepcopy(self.headers) )
                        )
        obj.credentials = deepcopy( self.credentials )
        return obj

    def _jsonloads( self, hdr, data ) :
        if 'application/json' in hdr.get( 'content-type', '' ) :
            data = JSON().decode( data.getvalue() )
        return data

    def _httpsession( self ):
        import couchpy.httpc
        return httpc.HttpSession()

    def _request( self, method, paths, headers, body, _query ):
        if isinstance(headers, dict) :
            all_headers = deepcopy( self.headers )
            all_headers.update( headers )
        paths = paths.split('/') if isinstance( paths, basestring ) else paths
        paths = filter( None, paths )
        url = urljoin( self.url, *paths, _query=_query )
        st = time.time()
        resp = self.htsess.request(
                    method, url, body=body, headers=all_headers,
                    credentials=self.credentials
               )
        log.info( "%6s %s %s (%s)" % (method, (time.time()-st), url, resp[0]) )
        return resp


    #---- HTTP method requests.

    def head( self, paths, hdrs, body, _query=[] ) :
        """HEAD request with http-headers ``hdrs`` and ``body``, for resource
        specified by base-url (provided while instantiation) and a list of
        path-segments ``paths``. Optional ``_query``, which is list of
        key,value tuples to construct url-query.

        Returns,
            HTTP response - status, headers, data
        """
        s, h, d = self._request('HEAD', paths, hdrs, body, _query)
        d = self._jsonloads( h, d ) if d != None else d
        return s, h, d

    def get( self, paths, hdrs, body, _query=[] ) :
        """GET request with http-headers ``hdrs`` and ``body``, for resource
        specified by base-url (provided while instantiation) and a list of
        path-segments ``paths``. Optional ``_query``, which is list of
        key,value tuples to construct url-query.

        Returns,
            HTTP response - status, headers, data
        """
        s, h, d = self._request('GET', paths, hdrs, body, _query)
        d = self._jsonloads( h, d )
        return s, h, d

    def post( self, paths, hdrs, body, _query=[] ) :
        """POST request with http-headers ``hdrs`` and ``body``, for resource
        specified by base-url (provided while instantiation) and a list of
        path-segments ``paths``. Optional ``_query``, which is list of
        key,value tuples to construct url-query.

        Returns,
            HTTP response - status, headers, data
        """
        s, h, d = self._request('POST', paths, hdrs, body, _query)
        d = self._jsonloads( h, d )
        return s, h, d


    def put( self, paths, hdrs, body, _query=[] ) :
        """PUT request with http-headers ``hdrs`` and ``body``, for resource
        specified by base-url (provided while instantiation) and a list of
        path-segments ``paths``. Optional ``_query``, which is list of
        key,value tuples to construct url-query.

        Returns,
            HTTP response - status, headers, data
        """
        s, h, d = self._request('PUT', paths, hdrs, body, _query)
        d = self._jsonloads( h, d )
        return s, h, d

    def delete( self, paths, hdrs, body, _query=[] ) :
        """DELETE request with http-headers ``hdrs`` and ``body``, for resource
        specified by base-url (provided while instantiation) and a list of
        path-segments ``paths``. Optional ``_query``, which is list of
        key,value tuples to construct url-query.

        Returns,
            HTTP response - status, headers, data
        """
        s, h, d = self._request('DELETE', paths, hdrs, body, _query)
        d = self._jsonloads( h, d )
        return s, h, d

    def copy( self, paths, hdrs, body, _query=[] ) :
        """COPY request with http-headers ``hdrs`` and ``body``, for resource
        specified by base-url (provided while instantiation) and a list of
        path-segments ``paths``. Optional ``_query``, which is list of
        key,value tuples to construct url-query.

        Returns,
            HTTP response - status, headers, data
        """
        s, h, d = self._request('COPY', paths, hdrs, body, _query)
        d = self._jsonloads( h, d )
        return s, h, d


    #---- Helper methods for couchpy modules that are related to framing HTTP
    #---- request.

    def savecookie( self, hthdrs, simplecookie ) :
        """Save ``simplecookie`` into http-request-headers ``hthdrs`` and
        return the same. ``hthdrs`` is expected to be a dictionary like object
        and ``simplecookie`` is expected by a cookie.SimpleCookie instance.
        """
        x = [ hthdrs.get( 'Set-Cookie', hthdrs.get( 'set-cookie', '' )) ]
        cookies = filter( None, x )
        for name, morsel in simplecookie.items() :
            cookies.append( '%s=%s' %  (name, morsel.value) )
        hthdrs['Cookie'] = ', '.join(cookies)
        return hthdrs

    def mixinhdrs( self, *hthdrs ) :
        newhthdrs = dict()
        [ newhthdrs.update(h) for h in hthdrs ]
        return newhthdrs


def urljoin( base, *paths, **kwargs ) :
    """Assemble a uri based on a base-url, any number of path segments
    ``paths``, and query key-word argument ``_query``, which is a list of
    key,value tuples.  Each path-segment in ``paths`` will be quoted using
    ``urllib.quote``. If value in _query (key,value) is a list or tuple, like,
    (key, [1,2,3]) will be expanded to [ (key,1), (key,2), (key,3) ]. Unicode
    query parameter values will be encoded in 'utf-8'. And the entire query
    string will be quoted using ``urllib.quote``

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

    # build pathinfo
    pathinfo = '/'.join( _quote(s) for s in paths if s )
    url = '/'.join([ base, pathinfo ])

    # build the query string
    params = []
    for name, value in _query :
        value = value if type(value) in (list, tuple) else [value]
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
    try :
        creds, netloc = netloc.split('@')
        credentials = tuple( urllib.unquote(i) for i in creds.split(':') )
        parts = [ parts[0] ] + [ netloc ] + parts[2:]
    except :
        credentials = None
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

def data2json( data ) :
    return JSON().encode( data or {} )
