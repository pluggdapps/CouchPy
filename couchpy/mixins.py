# This file is subject to the terms and conditions defined in
# file 'LICENSE', which is part of this source code package.
#       Copyright (c) 2011 SKR Farms (P) LTD.

# -*- coding: utf-8 -*-

class MixinDB( object ) :

    def deleteupdate( self, docs, key=None, merge=None ) :
        """Some document in documents `docs` can be fresh to database, while
        some other might by pointing to exising ones. For existing documents,
        read the database version and delete that version. Update the document
        with the new one create a fresh copy of the document.
        """
        # Fetch the documents that are already available in the database.
        key = key or ( lambda doc : doc['_id'] )
        keys = map( key, docs )
        dbdocs = self.docs( keys=keys ).get( 'rows', [] )
        dbids = [ doc['id'] for doc in dbdocs ]
        # Partition fresh documents from existing ones.
        fn = lambda p, x : p[0].append(x) if key(x) in dbids else p[1].append(x)
        partition = [ [], [] ]  # [ dbdocs, newdocs ]
        reduce( fn, partition )
        _, newdocs = partition
        # Compose documents to delete
        [ { '_id': d['_id'], '_rev': d['_rev'], '_deleted': True }
          for d in dbdocs ]
        d = self.bulkdocs( docs=deldocs )
        conflicts = [ r for r in d if r.get('error','') == 'conflict' ]
        if conflicts :
            log.error( 'Conflicts while delete db versions %s' % conflicts )
        # New documents
        d = self.bulkdocs( docs=newdocs )
        conflicts = [ r for r in d if r.get('error','') == 'conflict' ]
        if conflicts :
            log.error( 'Conflicts while updating merged versions %s' % conflicts )
        return d


class MixinDoc( object ) :
    def deleteupdate( self, doc, withdoc, fetch=False ) :
        """If `doc` is Document object, them merge the document with `withdoc`.
        Delete the original document in the database and create a new one
        using the merged doc.

        If `doc` is `_id`, or dictionary, use its _id to fetch the document
        from database. If the document is not present in the database, then
        simply create the document using `withdoc`.
        """
        id_ = doc if isinstance( doc, basestring ) else doc['_id']
        try :
            doc = Document( db, id_, fetch=True )
        except ResourceNotFound :
            newdoc = cls.create( db, newdoc, fetch=fetch )
        else :
            doc.pop( '_id', None ); doc.pop( '_rev', None )
            newdoc = deepcopy( doc.doc )
            newdoc.update( withdoc )
            cls.delete( db, doc )
            newdoc = cls.create( db, newdoc, fetch=fetch )
        return newdoc
