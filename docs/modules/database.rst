:mod:`couchpy.database` -- CouchDB database interface
=====================================================

.. automodule:: couchpy.database

Module Contents
---------------

.. autoclass:: Database
    :members: __new__, __init__, __call__, __iter__, __getitem__, __len__,
              __nonzero__, __delitem__, __eq__, __repr__, __contains__,
              ispresent, changes, compact, viewcleanup, ensurefullcommit,
              bulkdocs, bulkdelete, tempview, purge, all_docs, missingrevs,
              revsdiff, security, revslimit, Document, LocalDocument,
              DesignDocument, put, delete, fetch, commit
