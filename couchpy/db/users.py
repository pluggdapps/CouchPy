"""Database definition for ``_users``. It is a special database used by CouchDB
for user authentication.
"""

import time, datetime
from   random               import choice
from   hashlib              import sha1

from   couchpy.database     import Database
from   couchpy.doc          import Document

class UsersDB( Database ) :

    ALPHANUM = 'ABCDEFGHIJKLMNOPQRSTUVWXYZ1234567890'

    def __init__( self, *args, **kwargs ) :
        self.salt = kwargs.pop( 'salt', 'DEADBEAF' )
        Database.__init__( self, *args, **kwargs )

    def gensalt( self, randomsalt=True ) :
        if randomsalt == True :
            salt = ''
            for i in range(16) :
                a = choice(self.ALPHANUM) 
                a = choice([ a, a.lower() ])
                salt += a
        else :
            salt = self.salt
        return salt

    def register( self, name, password, randomsalt=True, **fields ) :
        """Register a new user in this database and return ``UserDoc``
        object for the registered user.

        fields can contain,

        ``firstname``,
        ``lastname``,
        ``timezone``,
        ``roles``
        """
        salt = self.gensalt(randomsalt=randomsalt)
        doc = {
            'type' : UserDoc.TYPE,
            '_id' : UserDoc.IDPREFIX + name,
            'name' : name,
            'salt' : salt,
            'password_sha' : sha1(password + salt).hexdigest(),
            'created_on' : time.mktime(datetime.datetime.utcnow().timetuple()),
            'updated_on' : time.mktime(datetime.datetime.utcnow().timetuple()),
            'roles' : fields.pop( 'roles', [] ),
        }
        doc.update(
            dict([ (k,v) for k,v in fields.items() if k in UserDoc.FIELDS ])
        )
        docobj = self.createdoc( docs=[doc], doc_cls=UserDoc )
        return docobj


class UserDoc( Document ) :
    IDPREFIX = 'org.couchdb.user:'
    TYPE = 'user'
    FIELDS = [ 'name', 'firstname', 'lastname', 'timezone', 'roles' ]

    def update( self, using={}, hthdrs={}, **query ) :
        password = using.pop( 'password', None )
        if password :
            using['salt'] = self.salt
            using['password_sha'] = sha1(password + self.salt).hexdigest()
        self.updateroles( using.pop( 'roles', None ))
        using = dict([ (k,v) for k,v in using.items() if k in UserDoc.FIELDS ])
        using.setdefault(
            'updated_on',
            time.mktime(datetime.datetime.utcnow().timetuple())
        )
        # If user's name has changed then the `_id` field must correspondingly
        # change.
        #if using['name'] != self.name :
        #    self.doc['_id'] = self.IDPREFIX + using['name']
        Document.update( self, using=using, hthdrs=hthdrs, **query )

    def updateroles( self, roles=None ) :
        roles = [roles] if isinstance( roles, basestring ) else roles
        if roles :
            self['roles'] = roles

    def _fromctime( self, t ) :
        try :
            x = time.ctime( float(x) )
        except :
            x = '-'
        return x

    #---- Document properties
    @property
    def created_on( self ) :
        return self._fromctime( self.doc['created_on'] )

    @property
    def updated_on( self ) :
        return self._fromctime( self.doc['updated_on'] )

    type = property( lambda self : self.doc['type'] )
    name = property( lambda self : self.doc['name'] )
    firstname = property( lambda self : self.doc['firstname'] )
    lastname = property( lambda self : self.doc['lastname'] )
    roles = property( lambda self : self.doc['roles'] )
    timezone = property( lambda self : self.doc['timezone'] )

    salt = property( lambda self : self.doc['salt'] )
    password_sha = property( lambda self : self.doc['password_sha'] )
