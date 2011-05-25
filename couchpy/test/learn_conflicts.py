from   copy                 import deepcopy
from   couchpy.client       import Client
from   couchpy.database     import Database
from   couchpy.doc          import Document
from   couchpy.view         import View
from   couchpy.query        import Query

URL = 'http://pratap:pratap@localhost:5984/'
c = Client( url=URL )
if 'db1' in c : del c['db1']
if 'db2' in c : del c['db2']
if 'db3' in c : del c['db3']

db1 = c.create( 'db1' )
db2 = c.create( 'db2' )
db3 = c.create( 'db3' )

fn = "function(doc) {if(doc._conflicts) { emit(doc._conflicts, null);} }"
db1.createdoc( docs=[{ "_id" : "_design/conflicts", "views": { "showconflicts": { "map": fn } }}] )[0]
db1doc = db1.createdoc( docs=[{ '_id' : 'test', 'count' : 1 }] )[0] # version 1 in db1
db1docs = [ deepcopy(db1doc.doc) ]

c.replicate( 'db1', 'db2' )                         # Replicate version 1 in db2
db2doc = Document( db2, 'test', fetch=True )
db2docs = [ deepcopy(db2doc.doc) ]
c.replicate( 'db1', 'db3' )                         # Replicate version 1 in db3
db3doc = Document( db3, 'test', fetch=True )
db3docs = [ deepcopy(db3doc.doc) ]

db1doc.update({ 'count' : db1doc['count']+1 })      # Update version 2 in db1
db1docs.append( deepcopy(db1doc.doc) )
db3doc.update({ 'count' : db3doc['count']+100 })    # Update version 2 in db3
db3docs.append( deepcopy(db3doc.doc) )

db2doc.update({ 'count' : db2doc['count']+10 })     # Update version 2 in db2
db2docs.append( deepcopy(db2doc.doc) )
db2doc.update({ 'count' : db2doc['count']+10 })     # Update version 3 in db2
db2docs.append( deepcopy(db2doc.doc) )

c.replicate( 'db2', 'db1' )                         # Conflict version 2, 3 in db1
c.replicate( 'db3', 'db1' )                         # Conflict version 2 in db1
c.replicate( 'db1', 'db2' )
c.replicate( 'db3', 'db2' )
c.replicate( 'db1', 'db3' )
c.replicate( 'db2', 'db3' )

v = View( db1, '_design/conflicts', 'showconflicts' )
conflicts1 = v( _q=Query() )
v = View( db2, '_design/conflicts', 'showconflicts' )
conflicts2 = v( _q=Query() )
v = View( db3, '_design/conflicts', 'showconflicts' )
conflicts3 = v( _q=Query() )

print db1docs
print db2docs
print db3docs
print '...........'
print db1doc( revs=True )
print db2doc( revs=True )
print db3doc( revs=True )
print '.............'
print conflicts1
print conflicts2
print conflicts3
