:mod:`couchpy.doc` -- CouchDB document interface
================================================

.. automodule:: couchpy.doc

Module Contents
---------------

.. autoclass:: StateMachine

.. autoclass:: Document
    :members: __new__, __init__, changed, invalidate, is_dirty, __getattr__,
              __setattr__, __setitem__, __delitem__, clear, update, setdefault,
              pop, popitem, __call__, __repr__, head, post, fetch, put, delete,
              copy, attach, attachments, Attachment

.. autoclass:: Attachment
    :members: __init__, get, put, delete

.. autoclass:: LocalDocument
    :members: __init__, __getattr__, __setattr__, __call__, fetch, put,
              delete, copy,

.. autoclass:: ImmutableDocument
    :members: __init__, __getattr__, __setattr__, __getitem__, __repr__, fetch

.. autoclass:: DesignDocument
    :members: __init__, info, views

.. autoclass:: Views
    :members: __init__, __getattr__, __setattr__

.. autoclass:: View
    :members: __init__, __call__, fetch

.. autoclass:: Query
    :members: __init__, __getattr__, __setattr__, __getitem__, __setitem__,
              __delitem__, clear, update, setdefault, pop, popitem, __call__,
              __repr__, __str__
