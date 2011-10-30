#!/usr/bin/env python

# Copyright (C) 2009 Christopher Lenz
# Copyright (C) 2011 Pratap Chakravarthy

# -*- coding: utf-8 -*-

class HTTPError(Exception):
    """Base class for errors based on HTTP status codes >= 400."""


class InvalidDBname(Exception) :
    """Exception raised when Database name is not valid"""


class RedirectLimit(Exception):
    """Exception raised when a request is redirected more often than allowed
    by the maximum number of redirections.
    """

class BadRequest(HTTPError):
    """400. The error can indicate an error with the request URL, path or
    headers. Differences in the supplied MD5 hash and content also trigger
    this error, as this may indicate message corruption.
    """


class Unauthorized(HTTPError):
    """401. Exception raised when the server requires authentication
    credentials but either none are provided, or they are incorrect.
    """


class Forbidden(HTTPError):
    """403. The requested item or operation is forbidden.
    """


class ResourceNotFound(HTTPError):
    """404. The requested content could not be found. The content will include
    further information, as a JSON object, if available. The structure
    will contain two keys, error and reason.
    """

class ResourceNotAllowed(HTTPError):
    """405. A request was made using an invalid HTTP request type for the URL
    requested. For example, you have requested a PUT when a POST is required.
    Errors of this type can also triggered by invalid URL strings.
    """


class NotAcceptable(HTTPError):
    """406. The requested content type is not supported by the server.
    """


class ResourceConflict(HTTPError):
    """409. Request resulted in an update conflict.
    """

class PreconditionFailed(HTTPError):
    """412. The request headers from the client and the capabilities of the
    server do not match.
    """

class BadContentType(HTTPError):
    """415. The content types supported, and the content type of the
    information being requested or submitted indicate that the content type is
    not supported.
    """


class RangeNotSatisfiable(HTTPError):
    """416. The range specified in the request header cannot be satisfied by
    the server.
    """


class ExpectationFailed(HTTPError):
    """417. When sending documents in bulk, the bulk load operation failed.
    """


class ServerError(HTTPError):
    """500. Exception raised when an unexpected HTTP error is received in
    response to a request.
    """


hterr_class = {
    400 : BadRequest,
    401 : Unauthorized,
    403 : Forbidden,
    404 : ResourceNotFound,
    405 : ResourceNotAllowed,
    406 : NotAcceptable,
    409 : ResourceConflict,
    412 : PreconditionFailed,
    415 : BadContentType,
    416 : RangeNotSatisfiable,
    417 : ExpectationFailed
}
