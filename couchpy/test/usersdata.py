# CouchPy Couchdb data-modeling for CouchDB database management systems
#   Copyright (C) 2011  SKR Farms (P) LTD
# 
# This program is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
# 
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
# 
# You should have received a copy of the GNU General Public License
# along with this program.  If not, see <http://www.gnu.org/licenses/>.

# -*- coding: utf-8 -*-

maxusers         = 200
minusers         = 10
no_of_userimages = 50
maxrelations     = 30

emaildomains = [
    'yahoo.co.in', 'gmail.com', 'yahoo.com', 'eudoramail.com', 'hotmail.com'
]
countries    = [ 'India' ]
states       = {
    'India' : [ 'TamilNadu', 'Karnataka', 'Kerala', 'AndhraPradesh', 'Goa',
                'Maharastra',
              ],
}
cities       = {
    'TamilNadu'     : [ 'Chennai', 'Kovai', 'Karur', 'Erode' ],
    'Karnataka'     : [ 'Bangalore', 'Mysore', 'Hubli', 'Belgaum' ],
    'Kerala'        : [ 'Cochin', 'Palghat', 'Calicut', 'Kottayam' ],
    'AndhraPradesh' : [ 'Guntur', 'Nandyal', 'Hyderabad', 'Vizag' ],
    'Goa'           : [ 'Panaji', 'Mapusa', 'Vosco' ],
    'Maharastra'    : [ 'Bombay', 'Pune', 'Nagpur' ],
}
areas        = {
    'Chennai'  : [ 'Triplicane', 'anna nagar', 'T nagar', 'Mylapore' ],
    'Kovai'    : [ 'RS Puram', 'Chokkampudur', 'GandhiPuram' ],
    'Karur'    : [ 'Anna-nagar', 'KVB-nagar', 'RamNagar', 'Manmangalam' ],
    'Bangalore': [ 'BTM Layout', 'Kodihalli', 'JP Nagar', 'Lingarajapuram' ],
    'Belgaum'  : [ 'cantonment' ],
    'Panaji'   : [ 'St Thomas Town' ],
    'Erode'    : [ 'Periyar nagar', 'Govindarajan Nagar' ],
}

names        = [
    'Aakarshan', 'Aancha', 'Aarthika', 'Adarna', 'Aarti', 'Aayesha', 'Abeer',
    'Abha', 'Babu', 'Abhay', 'Abhi', 'Abhijeet', 'Abhijit', 'Abhik',
    'Abhilasha', 'Abhinav', 'Abhishek', 'Abinash', 'Abraham', 'Adam',
    'Abrahamsen', 'Abu', 'Balasaheb', 'Achit', 'Achyuta', 'Adarsh',
    'Adarshpal', 'Adesh', 'Adina', 'Bakshi', 'Adit', 'Aditi', 'Aditya',
    'Adlakha', 'Badrinath', 'Adrianne', 'Aiyah', 'Ajai', 'Ajanta', 'Ajapa',
    'Ajatashatru', 'Ajay', 'Babulal', 'Aji', 'Ajinder', 'Ajitabh', 'Babul',
    'Badani', 'Badsah', 'Bagade', 'Bageshwari', 'Bagga', 'Bahadur', 'Baiju',
    'Baindur', 'Bakul', 'Bala', 'Balachandar', 'Balachandran', 'Balaji',
    'Balakrishna', 'Balakrishnan', 'Balasubramanian', 'Casukhela', 'Casula',
    'Cauvery', 'Chadaga', 'Chaitaly', 'Chaitanya', 'Chaitra', 'Chakradhar',
    'Chakrapani', 'Chalamala', 'Chalana', 'Daga', 'Dahana', 'Daksha',
    'Dakshesh', 'Daljit', 'Dalvie', 'Dama', 'Damodaragoun', 'Damodaran',
    'Dandapani', 'Dandekar', 'Dani', 'Edulbehram', 'Ekachakra', 'Eknath',
    'Ekta', 'Elango', 'Elayavalli', 'Emankum', 'Emankumar', 'Eswara',
    'Eswarapu', 'Falguni', 'Farhan', 'Farzana', 'Firaki', 'Gadde', 'Gade',
    'Gadepalli', 'Gagan', 'Gahlot', 'Gajapathi', 'Gajaren', 'Gajaweera',
    'Gajendra', 'Gajraj', 'Gala', 'Gaman', 'Gambhir', 'Halima', 'Hament',
    'Hanuman', 'Hanumant', 'Haran', 'Haranath', 'Harbajan', 'Harbir',
    'Hardik', 'Haresh', 'Hari', 'Haria', 'Hariharan', 'Ila', 'Ilango',
    'Ilanko', 'Ilyas', 'Imani', 'Ina', 'Inder', 'Inderpal', 'Indira', 'Indra',
    'Indrani', 'Indrayan', 'Indu', 'Indudeep', 'Indumathy', 'Innuganti',
    'Jadhav', 'Jagadish', 'Jagannath', 'Jagannathan', 'Jagarlamudi',
    'Jagdish', 'Jagganathan', 'Jagrati', 'Jagruti', 'Jahnavi', 'Jaideep',
    'Jaidev', 'Kaalki', 'Kabir', 'Kabra', 'Kachwaha', 'Kadak', 'Kadambi',
    'Kadamuddi', 'Kahn', 'Kaikini', 'Kailash', 'Kaisth', 'Kaith', 'Kajal',
    'Kajol', 'Kakde', 'Labhsha', 'Laddha', 'Lahiri', 'Lakhani', 'Macharla',
    'Macwan', 'Madan', 'Madanraj', 'Naagesh', 'Nabendu', 'Nachik',
    'Nachiketa', 'Om', 'Omar', 'Omarjeet', 'Omesh', 'Omkar', 'Oruganti',
    'Perumbeti', 'Phadnis', 'Phalgun', 'Phani', 'Phutika', 'Pichai',
    'Pillalamarri', 'Pivari', 'Piyush', 'Poduri', 'Podury', 'Polamreddy',
    'Polavarapu', 'Qamar', 'Rabinder', 'Rabindra', 'Rabindran', 'Rachna',
    'Rachoor', 'Radha', 'Radhakrish', 'Radhakrishna', 'Radhakrishnan',
    'Radhe', 'Radheshyam', 'Saandeep', 'Sabeena', 'Sabeer', 'Sabrina',
    'Sachchit', 'Sachdev', 'Sachi', 'Sachin', 'Sadalge', 'Sadaram',
    'Sadashiv', 'Sadayappan', 'Taksa', 'Talwar', 'Tamhane', 'Tammana',
    'Tamragouri', 'Tanmaya', 'Tantry', 'Tanu', 'Tanuj', 'Tanuja', 'Tanushi',
    'Tanvi', 'Tapan', 'Tapas', 'Ubriani', 'Uday', 'Udaya', 'Vadakke',
    'Vadlamani', 'Vaibhav', 'Waman', 'Yadavalli', 'Yadawa', 'Zahin',
    'Zankhana', 'Zarna', 'Zev',
] 

userimagedir   = os.path.join( sampledata_dir, 'userimages' )
userimagefiles = [( os.path.join( userimagedir, 'user'+str(i)+'_photo.jpg' ),
                    os.path.join( userimagedir, 'user'+str(i)+'_icon.jpg' )
                  ) for i in range(1,no_of_userimages+1) ]

def gen_usercontent( no_of_users=None, seed=None ) :
    """Generate content / data to fill user and userinfo tables.
    Return dictionary of dictionaries,
        { username : { 'username' : ..., 'firstname' : ..., ... },
          ....
        }
    """
    seed      = seed or genseed()
    random.seed( seed )
    # Dirty heuristics,
    userscomp   = h.fromconfig( 'userscomp' )
    # Only site permissions are allowed for a user.
    perm_groups = [ 'defgrp_'+pname.lower()
                    for pname in userscomp.site_permnames ]
    no_of_users   = no_of_users or randint( minusers, maxusers )
    maxpgroups    = len( perm_groups )
    minpgroups    = [ 0, maxpgroups - 3 ][ maxpgroups > 3 ]
    no_of_pgroups = randint( minpgroups, maxpgroups )
    userdata      = {}
    for i in range(no_of_users) :
        firstname  = unicode(choice( names ))
        middlename = unicode(choice( names ))
        lastname   = unicode(choice( names ))
        country    = unicode(choice( countries ))
        state      = unicode(choice( states[country] ))
        city       = unicode(choice( cities[state] ))
        username   = unicode(firstname + middlename[0] + lastname[0]).lower()
        d = { 'id'            : None,
              'username'      : username,
              'emailid'       : username + u'@' + choice( emaildomains ),
              'timezone'      : unicode(choice( all_timezones )),
              'password'      : username + '123',
              'disabled'      : choice([False,False,False,False,True]),
              'firstname'     : firstname,
              'middlename'    : middlename,
              'lastname'      : lastname,
              'city'          : unicode(city),
              'addressline1'  : str(randint(1,999)) + u', ' + \
                                str(randint(1,50)) + u' cross, ' + \
                                str(randint(1,50)) + ' main',
              'addressline2'  : ( city in areas and \
                                  unicode(choice( areas[city])) ) or u'',
              'country'       : country,
              'state'         : state,
              'pincode'       : unicode(randint( 100000, 999999 )),
              #'userpanes'     : choice([ u'favorites', u'wikis,tickets',
              #                           u'calendar,projects',
              #                           u'siteuserpanes' ]),
              'userpanes'     : u'siteuserpanes',
              'photofile'     : choice([0,1,1,1]) and \
                                userimagefiles[i % no_of_userimages][0],
              'iconfile'      : choice([0,1,1,1]) and \
                                userimagefiles[i % no_of_userimages][1],
              'perm_groups'   : list(set([ choice(perm_groups) 
                                           for i in range(no_of_pgroups) ])),
            }
        userdata.setdefault( username, d )
    return userdata

def gen_userrelations( usernames, userreltypes, no_of_relations=None, seed=None ) :
    """Generate user relationship data
    Return user relation data,
        { username : [ { 'userto' : <username>,
                         'userrel_type' : <reltype>,
                         'approved' : True or False,
                       }, 
                       ... ],
          ...
        }
    """
    seed      = seed or genseed()
    random.seed( seed )
    # remove anonymous user from the list.
    'anonymous' in usernames and usernames.remove( 'anonymous' )

    no_of_relations = no_of_relations or randint( 0, maxrelations )
    userreldata     = {}
    for user in usernames :
        potusers = usernames[:]
        potusers.remove( user )
        rels     = []
        for i in range(no_of_relations) :
            d = { 'id'           : None,
                  'userto'       : potusers.pop(potusers.index(choice(potusers))),
                  'userrel_type' : choice(userreltypes),
                  'approved'     : choice([ True,False ]),
                }
            rels.append( d )
        userreldata.setdefault( user, rels )
    return userreldata

##################### Generate license data ##########################
def gen_licenses( no_of_tags=None, no_of_attachs=None, seed=None ) :
    no_of_tags    = no_of_tags or randint(3,6)
    no_of_attachs = no_of_attachs or 1
    seed          = seed or genseed()
    random.seed( seed )
    lictextdir    = os.path.join( sampledata_dir, 'licensetext' )

    licdata = {
        u'Adaptive Public License'     : \
                { 'id'          : None,
                  'licensename' : u'Adaptive Public License', 
                  'summary'     : u'Adaptive Public License Version 1.0',
                  'source'      : u'Unknown',
                  'text'        : unicode( open( 
                                    os.path.join(
                                        lictextdir,
                                        'Adaptive-Public-License.txt'
                                    )).read()
                                  ),
                  'tags'        : gentags( no_of_tags, [ g_byuser ] ),
                  'attachs'     : genattachs( no_of_attachs, [ g_byuser ] ),
                },
        u'Apache Software License'     : \
                { 'id'          : None,
                  'licensename' : u'Apache Software License', 
                  'summary'     : u'License Version 2.0 by Apache Foundation',
                  'source'      : u'Apache Foundation',
                  'text'        : unicode( open(
                                    os.path.join(
                                        lictextdir,
                                        'Apache-Software-License.txt')
                                    ).read()
                                  ),
                  'tags'        : gentags( no_of_tags, [ g_byuser ] ),
                  'attachs'     : genattachs( no_of_attachs, [ g_byuser ] ),
                },
        u'Apple Public Source License' : \
                { 'id'          : None,
                  'licensename' : u'Apple Public Source License',
                  'summary'     : u'From Apple',
                  'source'      : u'Apple Computers',
                  'text'        : unicode( open( 
                                    os.path.join(
                                        lictextdir,
                                        'Apple-Public-Source-License.txt',
                                    )).read()
                                  ),
                  'tags'        : gentags( no_of_tags, [ g_byuser ] ),
                  'attachs'     : genattachs( no_of_attachs, [ g_byuser ] ),
                },
        u'Eclipse Public License'      : \
                { 'id'          : None,
                  'licensename' : u'Eclipse Public License',
                  'summary'     : u'Defined for Eclipse',
                  'source'      : u'Eclipse Foundation',
                  'text'        : unicode( open( 
                                    os.path.join(
                                        lictextdir,
                                        'Eclipse-Public-License.txt',
                                    )).read()
                                  ),
                  'tags'        : gentags( no_of_tags, [ g_byuser ] ),
                  'attachs'     : genattachs( no_of_attachs, [ g_byuser ] ),
                },
        u'The GNU General Public License' : \
                { 'id'          : None,
                  'licensename' : u'The GNU General Public License',
                  'summary'     : u'Version 3. This license governs most of the GNU software',
                  'source'      : u'GNU',
                  'text'        : unicode( open( 
                                    os.path.join(
                                        lictextdir,
                                        'The-GNU-General-Public-License.txt',
                                    )).read()
                                  ),
                  'tags'        : gentags( no_of_tags, [ g_byuser ] ),
                  'attachs'     : genattachs( no_of_attachs, [ g_byuser ] ),
                },
        u'GNU Lesser General Public License' : \
                { 'id'          : None,
                  'licensename' : u'GNU Lesser General Public License',
                  'summary'     : u'Version 2.1.  This license governs most' + \
                                   'of the Company contributed software towards GNU',
                  'source'      : u'GNU',
                  'text'        : unicode( open( 
                                    os.path.join(
                                        lictextdir,
                                        'GNU-Lesser-General-Public-License.txt',
                                    )).read()
                                  ),
                  'tags'        : gentags( no_of_tags, [ g_byuser ] ),
                  'attachs'     : genattachs( no_of_attachs, [ g_byuser ] ),
                },
        u'IBM Public License'            : \
                { 'id'          : None,
                  'licensename' : u'IBM Public License',
                  'summary'     : u'Version 1.0.',
                  'source'      : u'IBM',
                  'text'        : unicode( open( 
                                    os.path.join(
                                        lictextdir,
                                        'IBM-Public-License.txt',
                                    )).read()
                                  ),
                  'tags'        : gentags( no_of_tags, [ g_byuser ] ),
                  'attachs'     : genattachs( no_of_attachs, [ g_byuser ] ),
                },
        u'Microsoft Public License (Ms-PL)'  : \
                { 'id'          : None,
                  'licensename' : u'Microsoft Public License (Ms-PL)',
                  'summary'     : u'Indeed the big brother shows some friendliness.',
                  'source'      : u'Microsoft',
                  'text'        : unicode( open( 
                                    os.path.join(
                                        lictextdir,
                                        'Microsoft-Public-License-(Ms-PL).txt',
                                    )).read()
                                  ),
                  'tags'        : gentags( no_of_tags, [ g_byuser ] ),
                  'attachs'     : genattachs( no_of_attachs, [ g_byuser ] ),
                },
        u'Microsoft Reciprocal License (Ms-RL)'  : \
                { 'id'          : None,
                  'licensename' : u'Microsoft Reciprocal License (Ms-RL)',
                  'summary'     : u'Indeed the big brother shows some friendliness.',
                  'source'      : u'Microsoft',
                  'text'        : unicode( open( 
                                    os.path.join(
                                        lictextdir,
                                        'Microsoft-Reciprocal-License-(Ms-RL).txt',
                                    )).read()
                                  ),
                  'tags'        : gentags( no_of_tags, [ g_byuser ] ),
                  'attachs'     : genattachs( no_of_attachs, [ g_byuser ] ),
                },
        u'Mozilla Public License'            : \
                { 'id'          : None,
                  'licensename' : u'Mozilla Public License',
                  'summary'     : u'Version 1.1',
                  'source'      : u'Mozilla Foundation',
                  'text'        : unicode( open( 
                                    os.path.join(
                                        lictextdir,
                                        'Mozilla-Public-License.txt',
                                    )).read()
                                  ),
                  'tags'        : gentags( no_of_tags, [ g_byuser ] ),
                  'attachs'     : genattachs( no_of_attachs, [ g_byuser ] ),
                },
        u'Nokia Open Source License'         : \
                { 'id'          : None,
                  'licensename' : u'Nokia Open Source License',
                  'summary'     : u'Version 1.0a',
                  'source'      : u'Nokia',
                  'text'        : unicode( open( 
                                    os.path.join(
                                        lictextdir,
                                        'Nokia-Open-Source-License.txt',
                                    )).read()
                                  ),
                  'tags'        : gentags( no_of_tags, [ g_byuser ] ),
                  'attachs'     : genattachs( no_of_attachs, [ g_byuser ] ),
                },
        u'SUN Public License'                : \
                { 'id'          : None,
                  'licensename' : u'SUN Public License',
                  'summary'     : u'Version 1.0',
                  'source'      : u'SUN Microsystems',
                  'text'        : unicode( open( 
                                    os.path.join(
                                        lictextdir,
                                        'SUN-Public-License.txt',
                                    )).read()
                                  ),
                  'tags'        : gentags( no_of_tags, [ g_byuser ] ),
                  'attachs'     : genattachs( no_of_attachs, [ g_byuser ] ),
                },
    }
    return licdata


##################### Generate project data ##########################
_projcompnames = [ 'tcp', 'smtp', 'http', 'ftp', 'udp', 'atm', 'icmp',
                   'kernel', 'usb', 'pci', 'bluetooth', 'touch',
                   'raytracing', 'gui', 'vga', 'svga', 'regex', 'cap',
                   'nss_nis', 'cap', 'keyutils', 'sysfs', 'cfont',
                   'keyutils', 'ntfs-3g', 'sysfs', 'cfont', 'm-2', 'pamc',
                   'thread_db-1', 'cidn-2', 'memusage', 'pamc', 'thread_db',
                   'cidn', 'pam_misc', 'ulockmgr', 'com_err', 'ncurses',
                   'pam_misc', 'ulockmgr', 'com_err', 'ncurses', 'pam',
                   'usb-0', 'console', 'ncursesw', 'pam', 'usb-0', 'console',
                   'ncursesw', 'parted-1', 'usplash', 'ld-linux', 'crypt-2',
                   'nsl-2', 'parted-1', 'util-2', 'acl', 'crypt', 'nsl',
                   'pcprofile', 'util', 'acl', 'nss_compat-2', 'popt',
                   'uuid', 'anl-2', 'ctutils', 'nss_compat', 'popt', 'uuid',
                   'anl', 'ctutils', 'nss_dns-2', 'proc-3', 'volume_id',
                   'atm', 'devmapper', 'nss_dns', 'pthread-2', 'volume_id',
                   'atm', 'discover', 'nss_files-2', 'pthread', 'wrap',
                   'attr', 'discover', 'nss_files', 'readline', 'wrap',
                   'attr', 'dl-2', 'nss_hesiod-2', 'readline', 'x86', 'blkid',
                   'nss_hesiod', 'resolv-2', 'blkid', 'e2p',
                   'nss_mdns4_minimal', 'resolv', 'brlapi', 'e2p',
                   'nss_mdns4', 'rt-2', 'brlapi', 'ext2fs',
                   'nss_mdns6_minimal', 'BrokenLocale-2', 'ext2fs',
                   'nss_mdns6', 'SegFault', 'BrokenLocale', 'fuse',
                   'nss_mdns_minimal', 'selinux', 'bz2', 'fuse', 'nss_mdns',
                   'sepol', 'bz2', 'gcc_s', 'nss_nis-2', 'slang', 'bz2',
                   'history', 'nss_nisplus-2', 'slang', 'c-2', 'history',
                   'nss_nisplus'
                 ]

def gen_projusers( usernames, teamtypes, maxusers=10, seed=None  ) :
    """select users to be assigned to project teams
    Return dictionary of,
        { team_type     : [ user, user, .... ],
          ...
        }
    """
    # Dirty heuristics,
    compmgr   = h.fromconfig( 'compmgr' )
    seed and random.seed( seed )

    prjcomp   = ProjectComponent( compmgr )

    no_of_users = randint( 0, maxusers )
    unames      = usernames[:]
    d      = dict([ ( team_type, [] ) for team_type in teamtypes ])
    for team_type in teamtypes :
        c    = randint( 0, no_of_users )
        usrs = [ unames.pop(unames.index(choice( unames ))) 
                 for i in range(c) if unames ]
        no_of_users -= c
        d[team_type] = usrs
    return d

def gen_projteamperms( teamtypes, perm_groups, maxpgroups=None, seed=None ) :
    """select permission groups to be assigned to project teams
    Return dictionary of,
        { team_type     : [ perm_group, perm_group, .... ],
          ...
        }
    """
    seed and random.seed( seed )

    maxpgroups = maxpgroups or len(perm_groups)
    minpgroups = [ 0, maxpgroups - 3 ][ maxpgroups > 3 ]
    d = dict([ ( team_type, [] ) for team_type in teamtypes ])
    for team_type in teamtypes :
        c       = randint( minpgroups, maxpgroups )
        pgroups = list(set([ choice(perm_groups) for i in range( c ) ]))
        d[team_type] = pgroups
    return d
    
def gen_projuserperms( projusers, perm_groups, maxpgroups=None, seed=None ) :
    """select permission groups to be assigned to project users
    Return dictionary of,
        { username     : [ perm_group, perm_group, .... ],
          ...
        }
    """
    seed and random.seed( seed )

    maxpgroups  = maxpgroups or len(perm_groups)
    minpgroups  = [ 0, maxpgroups - 3 ][ maxpgroups > 3 ]
    projusers   = list(set(projusers))
    d = dict([ ( u, [] ) for u in projusers ])
    for u in projusers :
        c       = randint( minpgroups, maxpgroups )
        pgroups = list(set([ choice(perm_groups) for i in range( c ) ]))
        d[u]    = pgroups
    return d

def gen_projcomponents( projectname, projusers, maxcomps=10, no_of_tags=None,
                        seed=None ) :
    """Generate the components for projects,
    Return,
        { 'componentname' : { component details }
          ...
        }
    """
    no_of_tags = no_of_tags or randint(3,6)
    seed and random.seed( seed )

    projname   = projectname.strip('1234567890') 
    projfile   = os.path.join( sampledata_dir, 'projects',
                               projname + '.py' 
                             )
    code = compile( open( projfile ).read(), projfile, 'exec' )
    eval( code )

    no_of_comps = randint( 0, maxcomps )
    compnames   = _projcompnames[:]
    components  = {}
    for i in range( no_of_comps ) :
        compname = compnames.pop(compnames.index(choice(compnames)))
        components[compname] = \
            {
                'id'            : None,
                'componentname' : unicode( compname ),
                'description'   : choice( locals()[projname+'_compdesc'] ),
                'owner'         : choice( projusers ),
                'tags'          : gentags( no_of_tags, projusers ),
            }
    return components

cancel_remark = \
u"""The milestone is cancelled since most of the activities are moved to other
milestones."""
complete_remark = \
u"""The milestone is completed or the remaining the activities are migrated to
another milestone."""

def gen_projmilestones( projectname, projusers, maxmstn=10,
                        no_of_tags=None, seed=None ) :
    """Generate the milestones for projects,
    Return,
        { 'milestone_name' : { milestone details }
          ...
        }
    """
    no_of_tags = no_of_tags or randint(3,6)
    seed and random.seed( seed )

    projname   = projectname.strip('1234567890') 
    projfile   = os.path.join( sampledata_dir, 'projects', projname + '.py' )
    code       = compile( open( projfile ).read(), projfile, 'exec' )
    eval( code )

    no_of_mstn = randint( 0, maxmstn )
    milestones = {}
    for i in range( no_of_mstn ) :
        mstnname = projectname + u'-milestone' + str(i+1)
        if i % 2 :
            if i % 3 :
                status         = 'cancelled'
                closing_remark = cancel_remark
            else :
                status         = 'completed'
                closing_remark = complete_remark
        else :
            closing_remark = u''
            status         = None
        milestones[mstnname] =\
            {
                'id'             : None,
                'milestone_name' : mstnname,
                'description'    : choice( locals()[projname+'_mstndesc'] ),
                'due_date'       : future_duedate( 
                                        *dt.datetime.utcnow().timetuple() ),
                'closing_remark' : closing_remark,
                'status'         : status,
                'tags'           : gentags( no_of_tags, projusers ),
            }
    return milestones

def gen_projversions( projectname, projusers, maxvers=10, no_of_tags=None, seed=None ) :
    """Generate the versions for projects,
    Return,
        { 'version_name' : { version details }
          ...
        }
    """
    no_of_tags = no_of_tags or randint(3,6)
    seed and random.seed( seed )

    projname   = projectname.strip('1234567890') 
    projfile   = os.path.join( sampledata_dir, 'projects', projname + '.py' )
    code       = compile( open( projfile ).read(), projfile, 'exec' )
    eval( code )

    no_of_vers = randint( 0, maxvers )
    versions   = {}
    for i in range( no_of_vers ) :
        vername = projectname + u'-ver' + str(i+1)
        versions[vername] = \
            { 
                'id'           : None,
                'version_name' : vername,
                'description'  : choice( locals()[projname+'_verdesc'] ),
                'tags'         : gentags( no_of_tags, projusers ),
            }
    return versions

def gen_projects( usernames, perm_groups, teamtypes, licenses,
                  no_of_projects=None, no_of_tags=None, no_of_attachs=None,
                  seed=None ) :
    # Dirty heuristics,
    userscomp   = h.fromconfig( 'userscomp' )
    compmgr     = h.fromconfig( 'compmgr' )

    no_of_projects = no_of_projects or 10
    no_of_tags     = no_of_tags or randint(3,6)
    no_of_attachs  = no_of_attachs or randint(3,5)

    seed       = seed or genseed()
    random.seed( seed )

    prjcomp   = ProjectComponent( compmgr )

    teamtypes.remove( prjcomp.team_nomember )

    # Prun site-level permission names
    perm_groups = userscomp.projpgroups

    # remove anonymous user from the list.
    'anonymous' in usernames and usernames.remove( 'anonymous' )
    projsamples = {
        u'denethor'       : \
                { 'projectname' : u'denethor',
                  'summary'     : u'Python way of lexing and yaccing',
                  'mailing_list': [ u'denethor@googlegroups.com', ],
                  'ircchannel'  : [ u'denethor#netirc.com', ],
                },
        u'dwalin'   : \
                { 'projectname' : u'dwalin',
                  'summary'     : u'Open Source Web Browser to connect people',
                  'mailing_list': [ u'dwalin@googlegroups.com', ],
                  'ircchannel'  : [ u'dwalin#netirc.com', ],
                },
        u'elendil'    : \
                { 'projectname' : u'elendil',
                  'summary'     : u'Programming language or scripting language, you decide ...',
                  'mailing_list': [ u'elendil.dev@googlegroups.com', 
                                    u'elendil.users@googlegroups.com', ],
                  'ircchannel'  : [ u'pydev#netirc.com', u'pyuser#netirc.com', ],
                },
        u'legolas'       : \
                { 'projectname' : u'legolas',
                  'summary'     : u'Open Source C compiler',
                  'mailing_list': [ u'legolas@googlegroups.com', ],
                  'ircchannel'  : [ u'legolas#netirc.com', ],
                },
        u'treebeard': \
                { 'projectname' : u'treebeard',
                  'summary'     : u'ORM for python to naturalise database access',
                  'mailing_list': [ u'treebeard@googlegroups.com', ],
                  'ircchannel'  : [ u'treebeard#netirc.com', ],
                },
        u'greyhame'      : \
                { 'projectname' : u'greyhame',
                  'summary'     : u'Good for unit testing and regression testing',
                  'mailing_list': [ u'nosedev@googlegroups.com',
                                    u'greyhame@googlegroups.com',
                                    u'greyhame.bugs@googlegroups.com', ],
                  'ircchannel'  : [ u'greyhame#netirc.com', ],
                },
        u'orald': \
                { 'projectname' : u'orald',
                  'summary'     : u'The magic that make web applicable.',
                  'mailing_list': [ u'orlang@googlegroups.com', ],
                  'ircchannel'  : [ u'ordiscuss#netirc.com', ],
                },
        u'lockbearer'      : \
                { 'projectname' : u'lockbearer',
                  'summary'     : u'Templating language that is insanely fast',
                  'mailing_list': [],
                  'ircchannel'  : [],
                },
        u'dernhelm'      : \
                { 'projectname' : u'dernhelm',
                  'summary'     : u'A comprehensive web toolkit',
                  'mailing_list': [ u'dernhelmdev@googlegroups.com', ],
                  'ircchannel'  : [ u'dernhelm#netirc.com', ],
                },
        u'zeta'      : \
                { 'projectname' : u'zeta',
                  'summary'     : u'The magic wand.',
                  'mailing_list': [ u'zetadev@googlegroups.com',
                                    u'zeta.rep@googlegroups.com'
                                    u'zetauser@yahoogroups.com',
                                    u'zeta@yahoogroups.com', ],
                  'ircchannel'  : [ u'zetadiscuss#netirc.com' ],
                },
    }

    projimagedir   = os.path.join( sampledata_dir, 'projimages' )

    projectnames   = sorted( projsamples.keys() )
    if no_of_projects < len(projsamples) :
        projsamples = dict([ (k, projsamples[k])
                             for k in projectnames[:no_of_projects] ])
    elif no_of_projects > len(projsamples) :
        projd = {}
        for i in range(no_of_projects) :
            inst     = i and str(i) or ''
            projname = projectnames[ i % len(projectnames) ] 
            key   = projname + inst
            value = copy.deepcopy( projsamples[projname] )
            value['projectname'] = value['projectname'] + inst
            projd.update({ key : value }) 
        projsamples = projd

    projectnames = sorted( projsamples.keys() )
    projdata     = {}
    # Add component, milestone and version details.
    for i in range( no_of_projects ) :
        projectname= projectnames[i]

        projname   = projectname.strip('1234567890') 
        projfile   = os.path.join( sampledata_dir, 
                                   'projects', projname + '.py' )
        code       = compile( open( projfile ).read(), projfile, 'exec' )
        eval( code )
        description= locals()[projname+'_description']

        teams      = gen_projusers( usernames, teamtypes )
        admin      = choice( usernames )
        admin      = userscomp.get_user( admin )
        adminemail = choice( [ admin.emailid ]*5 + \
                             [ u'project.admin' + str(i) + '@somehost.com'
                               for i in range(1,5) ]
                           )
        projusers  = [ u for t in teams for u in teams[t] ] + [ admin.username ]
        uploader   = choice( projusers )
        d = {}
        d.update( projsamples[projectname] )
        d['id']          = None
        d['description'] = description
        d['disabled']    = choice([ True,False,False,False ])
        d['exposed']     = choice([ True,True,True,False ])
        d['admin_email'] = adminemail
        d['license']     = choice(licenses)
        d['admin']       = admin.username
        d['projusers']   = projusers
        d['components']  = gen_projcomponents( projectname, projusers )
        d['milestones']  = gen_projmilestones( projectname, projusers )
        d['versions']    = gen_projversions( projectname, projusers )
        d['projectteams']= teams
        d['teamperms']   = gen_projteamperms( teamtypes, perm_groups )
        d['projectperms']= gen_projuserperms( projusers, perm_groups )
        d['tags']        = gentags( no_of_tags, projusers )
        d['attachs']     = genattachs( no_of_attachs, projusers )
        d['logofile']    = [ os.path.join( projimagedir,
                                           projectname+'_logo.jpg' ),
                             uploader
                           ]
        d['iconfile']    = [ os.path.join( projimagedir,
                                           projectname+'_icon.jpg' ),
                             uploader
                           ]
        d['favusers']    = list(set([ choice(usernames)
                                      for i in range(randint(0,len(usernames)-1)) ]))
        projdata[projectname] = d
    return projdata

######################## Generate ticket contents #######################
def tck_description( comments ) :
    dlen  = randint( 10, 1000 )
    desc = u''
    while len(desc) < dlen :
        desc += choice( comments )
    return desc
    
def gen_tickets( no_of_tickets=None, no_of_tags=None, no_of_attachs=None,
                 seed=None ) :
    """Generate tickets,
    Return a list of dictionaries"""
    # Dirty heuristics,
    userscomp   = h.fromconfig( 'userscomp' )
    compmgr     = h.fromconfig( 'compmgr' )

    no_of_tickets = no_of_tickets or randint( 1,1000 )
    no_of_tags    = no_of_tags or randint(3,6)
    no_of_attachs = no_of_attachs or randint(3,5)

    seed       = seed or genseed()
    random.seed( seed )

    attachcomp= AttachComponent( compmgr )
    liccomp   = LicenseComponent( compmgr )
    tagcomp   = TagComponent( compmgr )
    prjcomp   = ProjectComponent( compmgr )
    tckcomp   = TicketComponent( compmgr )

    perm_groups  = userscomp.get_permgroup()
    users        = userscomp.get_user()
    projects     = prjcomp.get_project()
    components   = prjcomp.get_component()
    milestones   = prjcomp.get_milestone()
    versions     = prjcomp.get_version()
    tck_types    = tckcomp.get_tcktype()
    tck_status   = tckcomp.get_tckstatus()
    tck_severity = tckcomp.get_tckseverity()

    validusers   = filter( lambda u : u.username != 'anonymous', users )
    tickets = []
    for i in range(no_of_tickets) :
        p            = choice( projects )
        projname     = p.projectname.strip('1234567890') 
        projfile     = os.path.join( sampledata_dir, 'projects',
                                     projname + '.py' )
        code         = compile( open( projfile ).read(), projfile, 'exec' )
        eval( code )
        projcomments = locals()[projname+'_comments']
        desc         = tck_description( projcomments )
        summary      = desc[:50] + '...'
        projusers    = [ p.admin ] + [ pr.user for pr in p.team ]
        components   = p.components and \
                       list(set([ choice( p.components ) 
                                  for i in range(choice([0,1,1])) ])) or []
        milestones   = p.milestones and \
                       list(set([ choice( p.milestones )
                                  for i in range(choice([0,1,1])) ])) or []
        versions     = p.versions and \
                       list(set([ choice( p.versions )
                                  for i in range(choice([0,1,1])) ])) or []
        blockedby    = list(set([ randint( 1, no_of_tickets ) 
                                  for i in range( randint(0,5) ) ]))
        blocking     = list(set([ randint( 1, no_of_tickets )
                                  for i in range( randint(0,5) ) ]))
        parent       = randint( 1, no_of_tickets/5 )
        d = {
                'id'                : None,
                'summary'           : summary,
                'description'       : desc,
                'tck_typename'      : choice( tck_types ),
                'tck_severityname'  : choice( tck_severity ),
                'promptuser'        : choice( projusers ),
                'project'           : p,
                'components'        : components,
                'milestones'        : milestones,
                'versions'          : versions,
                'blockedby'         : choice([ blockedby, [], [] ]),
                'blocking'          : choice([ blocking, [], [] ]),
                'parent'            : parent,
        }
        d['favusers'] = list(set([ choice(validusers).username
                                    for i in range(randint(0,len(validusers)-1)) ]))
        d['voteup'] = []
        d['votedown'] = []
        for u in validusers :
            if choice([ True, False, False ]) :
                continue
            if choice([ True, False ]) :
                d['voteup'].append( u )
            else :
                d['votedown'].append( u )

        # Dirty heuristics,
        ticketresolv = h.fromconfig( 'zeta.ticketresolv' )
        # Ticket status history
        sthist = []
        for i in range( randint( 0, 10 ) ) :
            owner  = choice( projusers )
            status = tck_status.pop( 0 )
            if status.tck_statusname == u'new' :
                continue
            if status.tck_statusname not in ticketresolv :
                due_date = future_duedate( *dt.datetime.utcnow().timetuple() )
            dst = {
                'id'                : None,
                'tck_statusname'    : status.tck_statusname,
                'due_date'          : due_date,
                'owner'             : owner,
                'promptuser'        : (randint(0,4) and owner.username) or None,
            }
            tck_status.append( status )
            sthist.append( dst )
        d['statushistory']  = sthist
        # Ticket comments
        tckcmts = []
        for i in range( randint( 0, 20 ) ) :
            commentby = choice( users )
            dcmt = {
                'id'        : None,
                'text'      : choice( projcomments ),
                'commentby' : commentby,
            }
            tckcmts.append( dcmt )
        d['comments'] = tckcmts
        # Ticket comment replies, `id` starts from 1
        replies = []
        for i in range(0, len(tckcmts) ) :
            replies.append(
                ( i==0 and -1 ) or \
                ( randint(0,4) and randint(0,i-1) ) or -1
            )
        d['replies'] = replies
        d['tags']    = gentags( no_of_tags, [ u.username for u in projusers ])
        d['attachs'] = genattachs( no_of_attachs, 
                                   [ u.username for u in projusers ])
        tickets.append( d )
    return tickets

######################## Generate vcs contents ##########################

def gen_vcs( no_of_vcs=None, seed=None ) :
    """Generate vcs entries"""
    # Dirty heuristics,
    userscomp = h.fromconfig( 'userscomp' )
    compmgr   = h.fromconfig( 'compmgr' )
    svnurl    = h.fromconfig( 'svnurl' )
    bzrurl    = h.fromconfig( 'bzrurl' )
    hgurl     = h.fromconfig( 'hgurl' )
    prjcomp   = ProjectComponent( compmgr )
    vcscomp   = VcsComponent( compmgr )

    vcsoptions = [ u'svn', u'bzr', u'hg' ]
    project_repost = {
        'denethor'   : [ unicode(svnurl), unicode(bzrurl), unicode(hgurl) ],
        'dwalin'     : [ unicode(svnurl), unicode(bzrurl), unicode(hgurl) ],
        'elendil'    : [ unicode(svnurl), unicode(bzrurl), unicode(hgurl) ],
        'legolas'    : [ unicode(svnurl), unicode(bzrurl), unicode(hgurl) ],
        'treebeard'  : [ unicode(svnurl), unicode(bzrurl), unicode(hgurl) ],
        'greyhame'   : [ unicode(svnurl), unicode(bzrurl), unicode(hgurl) ],
        'orald'      : [ unicode(svnurl), unicode(bzrurl), unicode(hgurl) ],
        'lockbearer' : [ unicode(svnurl), unicode(bzrurl), unicode(hgurl) ],
        'dernhelm'   : [ unicode(svnurl), unicode(bzrurl), unicode(hgurl) ],
        'zeta'       : [ unicode(svnurl), unicode(bzrurl), unicode(hgurl) ]
    }


    no_of_vcs = no_of_vcs or randint( 0, len(prjcomp.get_project()) * 2 )

    seed       = seed or genseed()
    random.seed( seed )

    users      = userscomp.get_user()
    projects   = prjcomp.get_project()
    vcsdata    = []

    for i in range(no_of_vcs) :
        p        = choice(projects)
        projname = p.projectname.strip('1234567890') 
        typename = choice(vcsoptions)
        rooturl  = project_repost[ projname ][ vcsoptions.index(typename) ]
        d = {   'project'   : p.projectname,
                'type'      : typename,
                'name'      : p.projectname + u'-repo' + str(i),
                'rooturl'   : rooturl,
                'loginname' : None,
                'password'  : None
             }
        vcsdata.append( d )
    return vcsdata

def gen_repdirs( v=None ) :
    repdirs  = []
    if v :
        vrep     = va.open_repository(v)
        try :
            dirurls, fileurls = repwalk( vrep, [v.rooturl] )
        except :
            pass
        repdirs.extend( dirurls )
        while dirurls :
            dirurls, fileurls = repwalk( vrep, dirurls )
            repdirs.extend( dirurls )
    return repdirs

def gen_vcsmounts( no_of_vcs=None, seed=None ) :
    """Generate vcs mount entries"""
    no_of_mounts = no_of_vcs * 3

    userscomp = h.fromconfig( 'userscomp' )
    compmgr   = h.fromconfig( 'compmgr' )
    svnurl    = h.fromconfig( 'svnurl' )
    prjcomp   = ProjectComponent( compmgr )
    vcscomp   = VcsComponent( compmgr )

    vcslist   = vcscomp.get_vcs()
    mountdata = []
    a_repdirs = {}
    for i in range(no_of_mounts) :
        v        = vcslist and choice(vcslist) or None
        p        = v.project
        projname = p.projectname.strip('1234567890') 

        repdirs  = a_repdirs.get( v.rooturl, None )
        if v and repdirs == None :
            repdirs = gen_repdirs( v )
            a_repdirs.setdefault( v.rooturl, repdirs )

        if v and repdirs :
            d = { 'id'        : None,
                  'project'   : p.projectname,
                  'name'      : u'mount%s' % i,
                  'content'   : choice( vcscomp.mountcontents ),
                  'repospath' : choice(repdirs),
                  'vcs_id'    : v.id
                 }
            mountdata.append( d )
    return mountdata

    

######################## Generate wiki contents ##########################

def gen_wikifiles( project ) :
    projname   = project.projectname.strip('1234567890') 
    wikipagedir= os.path.join( sampledata_dir, 'wikipages', projname )
    wikifiles  = os.listdir( wikipagedir )
    # Minimum version and max version is swapped. Because, the same function
    # is used by gen_review()
    return [ ( f, randint(1,10), 1 ) for f in wikifiles ]

def gen_wiki( no_of_tags=None, no_of_attachs=None, seed=None ) :
    """Generate wikis"""
    # Dirty heuristics,
    userscomp   = h.fromconfig( 'userscomp' )
    compmgr     = h.fromconfig( 'compmgr' )

    no_of_tags    = no_of_tags or randint(3,6)
    no_of_attachs = no_of_attachs or randint(3,5)
    attachcomp = AttachComponent( compmgr )
    tagcomp    = TagComponent( compmgr )
    prjcomp    = ProjectComponent( compmgr )
    wikicomp   = WikiComponent( compmgr )

    seed       = seed or genseed()
    random.seed( seed )

    permgroups = userscomp.get_permgroup()
    users      = userscomp.get_user()
    projects   = prjcomp.get_project()
    wikitypes  = wikicomp.get_wikitype()
    validusers = filter( lambda u : u.username != 'anonymous', users )
    wikidata   = []
    for p in projects :
        projname     = p.projectname.strip( '1234567890' )
        projfile     = os.path.join( sampledata_dir, 'projects',
                                     projname + '.py' )
        code         = compile( open( projfile ).read(), projfile, 'exec' )
        eval( code )
        projcomments = locals()[projname+'_comments']

        wikifiles  = gen_wikifiles( p )
        projfile   = os.path.join( sampledata_dir, 'projects',
                                   projname + '.py' )
        code       = compile( open( projfile ).read(), projfile, 'exec' )
        eval( code )
        projusers  = [ p.admin ] + [ pr.user for pr in p.team ]
        wikipagedir= os.path.join( sampledata_dir, 'wikipages', projname )
        for wikifile, maxver, _m in wikifiles :
            url      = url_for( r_projwiki, projectname=p.projectname,
                                wurl=wikifile )
            summary  = choice( projcomments )[:20] + '...'
            d = {
                    'id'            : None,
                    'wikiurl'       : unicode(url),
                    'summary'       : unicode(summary),
                    'sourceurl'     : u'http://dev.discoverzeta.com/help/installation',
                    'wiki_typename' : choice( wikitypes ),
                    'creator'       : choice( projusers ),
                    'project'       : p,
                }
            wikifile  = os.path.join( wikipagedir, wikifile )
            d['file'] = wikifile
            d['favusers'] = list(set([ choice(validusers).username
                                  for i in range(randint(0,len(validusers)-1)) ]))
            d['voteup'] = []
            d['votedown'] = []
            for u in validusers :
                if choice([ True, False, False ]) :
                    continue
                if choice([ True, False ]) :
                    d['voteup'].append( u )
                else :
                    d['votedown'].append( u )
            # Wiki contents
            text         = open( os.path.join( wikipagedir, wikifile )).read()
            wikicontents = []
            for i in range( maxver ) :
                content = '\n'.join([ l for l in text.splitlines()
                                        if choice([1,1,1,1,1,1,0]) ])
                dcnt = {
                        'id'        : None,
                        'author'    : choice( projusers ),
                        'text'      : unicode(content),
                        'version'   : None,
                       }
                wikicontents.append( dcnt )
            # The last content can be full version.
            if wikicontents :
                wikicontents[-1]['text'] = unicode(text)
            d['contents'] = wikicontents
            # Wiki comments
            wikicomments = []
            for i in range(  randint(0, 15) ) :
                u = choice(users)
                dcmt = {
                        'id'        : None,
                        'commentby' : choice( users ),
                        'version_id': randint( 1, len(wikicontents) + 1 ),
                        'text'      : unicode(choice( projcomments ))
                       }
                wikicomments.append( dcmt )
            d['comments'] = wikicomments
            # Wiki comment replies, `id` starts from 1
            replies = []
            for i in range(0, len(wikicomments)) :
                replies.append(
                    ( i==0 and -1 ) or \
                    ( randint(0,4) and randint(0,i-1) ) or -1
                )
            d['replies'] = replies
            d['tags']    = gentags( no_of_tags, [ u.username for u in projusers ])
            d['attachs'] = genattachs( no_of_attachs, 
                                       [ u.username for u in projusers ])
            wikidata.append( d )
    return wikidata

########################## Generate review contents ##################
def repwalk( vrep, dirs ) :
    listing = {}
    for d in dirs :
        try :
            urls = vrep.list( d )
        except :
            continue
        for f in urls :
            listing.setdefault( f[1], [] ).append( f[2] )

    return (listing.get( 'text/directory', [] ), listing.get( 'text/file', [] ))

def revisionrange( vrep, fileurls ) :
    data = []
    for f in fileurls :
        logs  = vrep.logs( f )
        if logs == [] : continue
        logs  = sorted( logs, key=lambda x : x[1] )
        begin = logs[0][1]
        end   = logs[-1][1]
        data.append( (f, begin, end) )
    return data

def gen_reviewurls( project, types=['wiki', 'vcsfile', 'vcsweburl'] ) :
    """Generate resource url"""
    urls = []
    # Add possible vcs sources via repository
    for vcs in project.vcslist :
        vcsurls = []
        vrep = va.open_repository( vcs )
        try :
            dirurls, fileurls = repwalk( vrep, [ vcs.rooturl ] )
        except :
            continue
        vcsurls.extend( revisionrange( vrep, fileurls ))
        while dirurls :
            dirurls, fileurls = repwalk( vrep, dirurls )
            vcsurls.extend( revisionrange( vrep, fileurls ))
        # Add possible vcs sources via website
        if 'vcsfile' in types :
            urls.extend( vcsurls )
        if 'vcsweburl' in types :
            weburls = [ ( url_for( r_projvcsfile, 
                                   projectname=project.projectname, vcsid=vcs.id,
                                   filepath=url[0].split(vcs.rooturl)[1].lstrip('/')
                                 ),
                          url[1], url[2]
                        ) for url in vcsurls ]
            urls.extend( weburls )
    # Add possible wiki source urls via website
    if 'wiki' in types :
        urls.extend(
            [ ( url_for( r_projwiki, projectname=project.projectname, 
                         wurl=wfile ),
                minver, maxver
              ) for wfile, maxver, minver in gen_wikifiles( project ) ]
        )
    return urls

def gen_reviews( rnatures, ractions, no_of_reviews=None, no_of_tags=None, 
                 no_of_attachs=None, seed=None ) :
    """Generate reviews"""
    # Dirty heuristics,
    userscomp   = h.fromconfig( 'userscomp' )
    compmgr     = h.fromconfig( 'compmgr' )

    no_of_reviews = no_of_reviews or randint( 0, 20 )
    no_of_tags    = no_of_tags or randint(3,6)
    no_of_attachs = no_of_attachs or randint(3,5)
    prjcomp       = ProjectComponent( compmgr )
    vcscomp       = VcsComponent( compmgr )

    seed       = seed or genseed()
    random.seed( seed )

    users      = userscomp.get_user()
    projects   = prjcomp.get_project()
    rsets      = [ comp.componentname for comp in prjcomp.get_component() ]

    # Generate review urls
    files = {}
    for p in projects :
        files[p.projectname] = gen_reviewurls( p )

    v_projects = [ p for p in projects if p.vcslist ]
    validusers = filter( lambda u : u.username != 'anonymous', users )
    reviews    = []
    for i in range( no_of_reviews ) :
        p   = choice( v_projects )

        projusers    = [ p.admin ] + [ pr.user for pr in p.team ]
        participants = list(set([ choice( users )
                                  for i in range( randint(0,5) ) ]))
        projname     = p.projectname.strip( '1234567890' )
        projfile     = os.path.join( sampledata_dir, 'projects',
                                     projname + '.py' )
        code         = compile( open( projfile ).read(), projfile, 'exec' )
        eval( code )
        projcomments = locals()[projname+'_comments']

        file = choice(files[p.projectname])

        d = {
            'id'            : None,
            'resource_url'  : unicode(file[0]),
            'version'       : file[1] == file[2] and file[1] \
                              or randint( file[1], file[2] ),
            'project'       : p,
            'author'        : choice( projusers ),
            'moderator'     : choice( projusers ),
            'participants'  : participants,
            'reviewset'     : choice( rsets ) if choice([0,1]) else None,
        }
        d['favusers'] = list(set([ choice(validusers).username
                                    for i in range(randint(0,len(validusers)-1)) ]))
        usercommentors = [ d['author'], d['moderator'] ] + d['participants']
        # Review comments    
        revcmts       = []
        for i in range( randint(0,10) ) :
            drcmt = {
                        'id'           : None,
                        'position'     : randint( 1,50 ),
                        'text'         : choice( projcomments ),
                        'approved'     : choice( [True,False] ),
                        'commentby'    : choice( usercommentors ),
                        'reviewnature' : choice( rnatures ),
                        'reviewaction' : choice( ractions ),
                    }
            revcmts.append( drcmt )
        d['comments'] = revcmts
        # Review comment replies, `id` starts from 1
        replies = []
        for i in range(0, len(revcmts)) :
            replies.append(
                ( i==0 and -1 ) or \
                ( randint(0,4) and randint(0,i-1) ) or -1
            )
        d['replies'] = replies
        d['closed']  = choice([True,False])
        d['tags']    = gentags( no_of_tags, [ u.username for u in projusers ])
        d['attachs'] = genattachs( no_of_attachs, 
                                   [ u.username for u in projusers ])
        reviews.append( d )
    return reviews
        
