
class Helpers( object ) :

    def savecookie( self, hthdrs, simplecookie ) :
        """Save `simplecookie` into http-request-headers `hthdrs`"""
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
