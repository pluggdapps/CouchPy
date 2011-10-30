# This file is subject to the terms and conditions defined in
# file 'LICENSE', which is part of this source code package.
#       Copyright (c) 2011 SKR Farms (P) LTD.

from   setuptools    import setup, find_packages
from   os.path       import abspath, dirname, join
import os
import re

here = abspath(dirname(__file__))
README = open(join(here, 'README.rst')).read()

v = open(join(dirname(__file__), 'couchpy', '__init__.py'))
version = re.compile(r".*__version__[ ]*=[ ]*'(.*?)'", re.S).match(v.read()).group(1)
v.close()

description="Data modeling for CouchDB database management systems"

classifiers=[
'Development Status :: 4 - Beta',
'Environment :: Console',
'Environment :: Plugins',
'Environment :: Web Environment',
'Framework :: Pylons',
'Intended Audience :: Developers',
'Intended Audience :: Education',
'Intended Audience :: End Users/Desktop',
'Intended Audience :: Information Technology',
'Intended Audience :: Science/Research',
'Intended Audience :: System Administrators',
'License :: OSI Approved :: BSD License',
'Natural Language :: English',
'Operating System :: MacOS :: MacOS X',
'Operating System :: Microsoft :: Windows :: Windows CE',
'Operating System :: Microsoft :: Windows :: Windows NT/2000',
'Operating System :: POSIX',
'Operating System :: Unix',
'Programming Language :: JavaScript',
'Programming Language :: Python :: 2.5',
'Programming Language :: Python :: 2.6',
'Programming Language :: Python :: 2.7',
'Topic :: Documentation',
'Topic :: Internet',
'Topic :: Utilities',
]

setup(
    name='couchpy',
    version=version,
    py_modules=[],
    package_dir={},
    packages=find_packages(),
    ext_modules=[],
    scripts=[],
    data_files=[],
    package_data={},                        # setuptools / distutils
    include_package_data=True,              # setuptools
    exclude_package_data={},                # setuptools
    zip_safe=True,                          # setuptools
    entry_points={                          # setuptools
    },
    install_requires=[                      # setuptools
        'pygments',
        'sphinx',
        'sphinx-pypi-upload',
    ],
    extras_require={},                      # setuptools
    setup_requires={},                      # setuptools
    dependency_links=[],                    # setuptools
    namespace_packages=[],                  # setuptools
    test_suite='couchpy.test',              # setuptools

    provides=[ 'couchpy', ],
    requires='',
    obsoletes='',

    author='Pratap R Chakravarthy',
    author_email='prataprc@gmail.com',
    maintainer='Pratap R Chakravarthy',
    maintainer_email='prataprc@gmail.com',
    url='http://couchpy.pluggdapps.com',
    download_url='',
    license='Original BSD license',
    description=description,
    long_description=README,
    platforms='',
    classifiers=classifiers,
    keywords=[ 'ORM database couchdb nosql modeling' ],
)
