#!/usr/bin/env python

# Copyright (C) 2009 Christopher Lenz
# Copyright (C) 2011 Pratap Chakravarthy

# -*- coding: utf-8 -*-

"""HTTP client wrapper around stdlib's ``httplib`` module."""

import sys, socket, time, errno, logging
from   urlparse         import urlsplit, urlunsplit
from   base64           import b64encode
from   datetime         import datetime
from   httplib          import BadStatusLine, HTTPConnection, HTTPSConnection

from   couchpy.utils    import JSON

try:
    from cStringIO      import StringIO
except ImportError:    
    from StringIO       import StringIO

try:
    from threading       import Lock
except ImportError:
    from dummy_threading import Lock

from   httperror import *

log = logging.getLogger( __name__ )

CHUNK_SIZE = 1024 * 8
CACHE_SIZE = 10, 75         # some random values to limit memory use

RETRYABLE_ERRORS = frozenset([
    errno.EPIPE,      errno.ETIMEDOUT,
    errno.ECONNRESET, errno.ECONNREFUSED, errno.ECONNABORTED,
    errno.EHOSTDOWN,  errno.EHOSTUNREACH,
    errno.ENETRESET,  errno.ENETUNREACH,  errno.ENETDOWN
])

# HTTP Status code.
OK          = 200
CREATED     = 201
ACCEPTED    = 202
NO_CONTENT  = 204
MOVED_PERMANENTLY  = 301
FOUND              = 302
SEE_OTHER          = 303
NOT_MODIFIED       = 304
TEMPORARY_REDIRECT = 307
BAD_REQUEST               = 400
UNAUTHORIZED              = 401
FORBIDDEN                 = 403
NOT_FOUND                 = 404
RESOURCE_NOT_ALLOWED      = 405
NOT_ACCEPTABLE            = 406
CONFLICT                  = 409
PRECONDITION_FAILED       = 412
BAD_CONTENT_TYPE          = 415
REQ_RANGE_NOT_SATISFIABLE = 416
EXPECTATION_FAILED        = 417
INTERNAL_SERVER_ERROR           = 500

def cache_sort(i):
    d = i[1][1]['Date'][5:-4]
    t = time.mktime(time.strptime(d, '%d %b %Y %H:%M:%S'))
    return datetime.fromtimestamp(t)

class HttpSession( object ) :

    _allowed_methods = ( 'GET', 'HEAD', 'PUT', 'POST', 'DELETE', 'COPY' )

    def __init__( self, cache=None, timeout=None, max_redirects=5,
                  retry_delays=[0], retryable_errors=RETRYABLE_ERRORS,
                  user_agent='couchpy' ) :
        """Initialize an HTTP client session.

        cache
            an instance with a dict-like interface or None to allow
            HttpSession to create a dict for caching.
        timeout
            socket timeout in number of seconds, or `None` for no timeout
        retry_delays
            list of request retry delays.
        """
        from couchpy   import __version__ as VERSION

        # Make a copy of constructor options
        self.user_agent = '%s-%s' % (user_agent, VERSION)
        self.cache = cache or {}
        self.timeout = timeout
        self.max_redirects = max_redirects
        self.retry_delays = list(retry_delays) # We don't want this changing.
        self.retryable_errors = set(retryable_errors)

        # Initialize object-attributes
        self.perm_redirects = {}
        self.conns = {} # HTTP connections keyed by (scheme, host)
        self.lock = Lock()

    def request( self, method, url, body=None, headers=None, credentials=None,
                 num_redirects=0, chunk_cb=None ) :
        """Handle a request
        :method ::
            HTTP method, GET, PUT, POST, DELETE, ALL
        :url ::
            CouchDB url
        :headers ::
            A dictionary of http headers, like `Content-type` and `Accept`
            ``Content-type``
            The use of the Content-type on a request is highly recommended.
            When uploading attachments it should be the corresponding MIME
            type for the attachment or binary ``application/octet-stream``
            ``Accept``
            Again highly recommended.
        """

        # Sanitize arguments
        method = method.upper()
        url = self.perm_redirects.get( url, url )
        headers = headers or {}
        basicauth = self.basicauth( credentials ) if credentials else None
        retries = iter(self.retry_delays)
        json    = JSON()

        if method not in self._allowed_methods :
            raise HTTPError( 'Method ``%s`` not allowed' % method )

        # Process request
        headers.setdefault( 'Accept', 'application/json' )
        headers['User-Agent'] = self.user_agent
        h, body = self.preprocess( method, url, body, basicauth )
        headers.update(h)

        conn = self.obtain_connection(url)
        resp = self.try_request_with_retries(
                        conn, retries, method, url, headers, body )
        status = resp.status

        req = Dummy()
        req.conn, req.url, req.method, req.headers, req.body = \
                conn, url, method, headers, body

        # Handle NOT_MODIFIED
        #rc = self.resp_not_modified(resp, req)
        #if rc != None : return rc

        #self.cache.pop( url, None )

        # Handle redirects
        rc = self.resp_redirect(resp, req, num_redirects)
        if rc != None : return rc

        data = None
        streamed = False

        # Read the full response for empty responses so that the connection is
        # in good state for the next request
        conds = [ method == 'HEAD',
                  resp.getheader('content-length') == '0', 
                  status < OK,
                  status in (NO_CONTENT, NOT_MODIFIED) ]
        if any(conds) :
            resp.read()
            self.release_connection(url, conn)

        # Buffer small non-JSON response bodies
        elif int(resp.getheader('content-length', sys.maxint)) < CHUNK_SIZE:
            data = resp.read()
            self.release_connection(url, conn)

        # For large or chunked response bodies, do not buffer the full body,
        # and instead return a minimal file-like object
        else :
            close_cb = lambda: self.release_connection(url, conn)
            data = ResponseBody( resp, close_cb, chunk_cb=chunk_cb )
            streamed = True

        # Handle errors
        if status >= BAD_REQUEST :  # 400
            ctype = resp.getheader('content-type')
            if data is not None and 'application/json' in ctype:
                data = json.decode( data )
                error = data.get('error'), data.get('reason')
            elif method != 'HEAD':
                error = resp.read()
                self.release_connection(url, conn)
            else:
                error = ''

            cls = hterr_class.get( status, None )
            if cls :
                raise cls(error)
            else :
                raise ServerError((status, error))

        # Store cachable responses
        #if not streamed and method == 'GET' and 'etag' in resp.msg:
        #    self.cache[url] = (status, resp.msg, data)
        #    self.clean_cache() if len(self.cache) > CACHE_SIZE[1] else None

        if not streamed and data is not None:
            data = StringIO(data)

        return status, resp.msg, data

    def clean_cache(self):
        ls = sorted(self.cache.iteritems(), key=cache_sort)
        self.cache = dict(ls[-CACHE_SIZE[0]:])

    def sendchunks( conn, body ) :
        while True :
            chunk = body.read(CHUNK_SIZE)
            if not chunk: break
            conn.send( ('%x\r\n' % len(chunk)) + chunk + '\r\n' )
        conn.send( '0\r\n\r\n' )

    def try_request( self, conn, method, url, headers={}, body=None ) :
        path_query = urlunsplit(('', '') + urlsplit(url)[2:4] + ('',))
        try:
            conn.putrequest( method, path_query, skip_accept_encoding=True )
            [ conn.putheader(f, val) for f, val in headers.iteritems() ]
            conn.endheaders()
            if isinstance(body, basestring):
                conn.send( body )
            elif body is not None : # assume a file-like object and ...
                self.sendchunks(conn, body)
            return conn.getresponse()

        except BadStatusLine, e:
            # httplib raises a BadStatusLine when it cannot read the status
            # line saying, "Presumably, the server closed the connection
            # before sending a valid response."
            # Raise as ECONNRESET to simplify retry logic.
            if e.line == '' or e.line == "''":
                raise socket.error(errno.ECONNRESET)
            else:
                raise


    def try_request_with_retries(self, conn, retries, method, url, hdr, body) :
        while True:
            try:
                return self.try_request(conn, method, url, hdr, body)
            except socket.error, e:
                ecode = e.args[0]
                if ecode not in self.retryable_errors:
                    raise
                try:
                    delay = retries.next()
                except StopIteration: # No more retries
                    raise e
                time.sleep( delay )
                conn.close()

    def preprocess( self, method, url, body, basicauth ) :
        """Pre-process a request"""
        headers = {}

        # Cached Etag
        #if method in ('GET', 'HEAD') :
        #    etag = self.cache.get( url, [None, {}] )[1].get( 'etag', None )
        #    headers.update({ 'If-None-Match' : etag }) if etag else None

        # JSON body
        if body and (not isinstance( body, basestring ) ) :
            json = JSON()
            try : body = json.encode( body )
            except TypeError : pass
            headers.setdefault('Content-Type', 'application/json')
        # Content-length, Transfer-Encoding
        headers.setdefault( 'Content-Length', '0' ) if body is None else None
        headers.setdefault( 'Content-Length', str(len(body)) 
        ) if isinstance( body, basestring
        ) else headers.update({ 'Transfer-Encoding' : 'chunked' })
        # Basc-Authorization
        headers.update({ 'Authorization': basicauth }) if basicauth else None

        return headers, body


    def basicauth( self, creds ) :
        return 'Basic %s' % b64encode('%s:%s' % creds) if creds else ''


    httpconncls = {
        'http'  : HTTPConnection,
        'https' : HTTPSConnection
    }
    def httpconnect( self, scheme, host ) :
        if scheme not in self.httpconncls.keys() :
            raise ValueError( '%s is not a supported scheme' % scheme )
        cls = self.httpconncls.get( scheme )
        conn = cls( host )
        conn.connect()
        return conn

    def obtain_connection( self, url ) :
        scheme, host = urlsplit( url, 'http', False )[:2]
        self.lock.acquire()
        try:
            conns = self.conns.setdefault((scheme, host), [])
            conn = conns.pop(-1) if conns else self.httpconnect( scheme, host )
        finally:
            self.lock.release()
        return conn

    def release_connection( self, url, conn ) :
        scheme, host = urlsplit( url, 'http', False )[:2]
        self.lock.acquire()
        try:
            self.conns.setdefault((scheme, host), []).append(conn)
        finally:
            self.lock.release()

    def resp_not_modified(self, resp, req) :
        status = resp.status
        conn, url, method = req.conn, req.url, req.method

        # Handle conditional response
        if status == NOT_MODIFIED  and method in ('GET', 'HEAD') :
            resp.read()
            self.release_connection( url, conn )
            status, msg, data = self.cache.get( url )
            data = StringIO(data) if data is not None else data
            return status, msg, data
        else :
            return None

    def resp_redirect(self, resp, req, num_redirects) :
        # Handle redirects
        status = resp.status
        conn, url, method, body, headers = \
                req.conn, req.url, req.method, req.body, req.headers

        s_ = (MOVED_PERMANENTLY, FOUND, TEMPORARY_REDIRECT)
        if (status == SEE_OTHER) or \
           (method in ('GET', 'HEAD') and status in s_) :
            resp.read()
            self.release_connection(url, conn)
            if num_redirects > self.max_redirects :
                raise RedirectLimit('Redirection limit exceeded')
            location = resp.getheader('location')
            if status == MOVED_PERMANENTLY :
                self.perm_redirects[url] = location
            elif status == SEE_OTHER :
                method = 'GET'
            return self.request( method, location, body, headers,
                                 num_redirects=num_redirects + 1 )
        else :
            return None


class ResponseBody( object ) :

    def __init__( self, resp, close_cb, chunk_cb=None ) :
        self.resp = resp
        self.close_cb = close_cb
        self.chunk_cb = chunk_cb

    def read( self, size=None ) :
        content = self.resp.read(size)
        self.close() if (size is None) or (len(content)<size) else None
        return content

    def close( self ) :
        while not self.resp.isclosed() : self.read(CHUNK_SIZE)
        self.close_cb()

    def getvalue( self ) :
        if self.chunk_cb and \
           (self.resp.msg.get('transfer-encoding') == 'chunked') :
            [ self.chunk_cb(l) for l in self ]
            content = ''
        else :
            content = self.resp.read()
            self.close()
        return content

    def __iter__( self ) :
        assert self.resp.msg.get('transfer-encoding') == 'chunked'
        while True :
            chunksz = int(self.resp.fp.readline().strip(), 16)
            if not chunksz :
                self.resp.fp.read(2) #crlf
                self.resp.close()
                self.close_cb()
                break
            chunk = self.resp.fp.read(chunksz)
            for ln in chunk.splitlines():
                yield ln
            self.resp.fp.read(2) #crlf

class Dummy( object ) :
    pass
