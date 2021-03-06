develop :
	rm -rf couchpy-env
	virtualenv couchpy-env --no-site-packages
	bash -c "source couchpy-env/bin/activate ; python ./setup.py develop"

testall :
	cd couchpy/test/; python ./testjson.py
	cd couchpy/test/; python ./test_client.py
	cd couchpy/test/; python ./test_database.py
	cd couchpy/test/; python ./test_doc.py

bdist_egg : copy
	python ./setup.py bdist_egg

sdist : copy
	python ./setup.py sdist

upload : copy
	python ./setup.py sdist register -r http://www.python.org/pypi upload -r http://www.python.org/pypi --show-response 
	
copy :
	cp CHANGELOG docs/CHANGELOG
	cp LICENSE docs/LICENSE
	cp README docs/README
	cp ROADMAP docs/ROADMAP

sphinxdoc :
	rm -rf docs/_build
	bash -c "source couchpy-env/bin/activate; cd docs ; make html"

upload-doc :
	python ./setup.py upload_sphinx -r http://www.python.org/pypi 

cleanall : clean cleandoc
	rm -rf couchpy-env

cleandoc : 
	rm -rf docs/_build
	rm -rf docs/_static

clean :
	rm -rf build;
	rm -rf dist;
	rm -rf couchpy.egg-info;
	rm -rf docs/CHANGELOG
	rm -rf docs/LICENSE
	rm -rf docs/README
	rm -rf docs/ROADMAP
	rm -rf `find ./ -name "*.pyc"`;
	rm -rf `find ./ -name "yacctab.py"`;
	rm -rf `find ./ -name "lextab.py"`;
