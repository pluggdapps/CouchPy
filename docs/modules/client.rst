:mod:`couchpy.client` -- CouchDB client
=======================================

.. automodule:: couchpy.client

Module Contents
---------------

.. autoclass:: Client
    :members: __init__, __contains__, __iter__, __len__, __nonzero__, __repr__,
              __delitem__, __getitem__, __call__, version, ispresent,
              active_tasks, all_dbs, log, restart, stats, uuids, config,
              addadmin, deladmin, admins, login, logout, authsession, put,
              delete, has_database, Database, DatabaseIterator, commit, replicate
