:mod:`couchpy.database` -- CouchDB database interface
=====================================================

.. automodule:: couchpy.database

Module Contents
---------------

.. autoclass:: Database
    :members: __init__, __call__, __iter__, __getitem__, __len__, __nonzero__,
              __delitem__, __eq__, __repr__, __contains__, ispresent, changes,
              compact, viewcleanup, ensurefullcommit, bulkdocs, tempview,
              purge, docs, revslimit, createdoc, deletedoc, designdocs,
              copydoc, create, delete, missingrevs, revsdiff, security,
              committed_update_seq, compact_running, disk_format_version,
              disk_size, doc_count, doc_del_count, instance_start_time, 
              purge_seq, update_seq
