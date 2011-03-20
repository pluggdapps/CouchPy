develop :
	rm -rf couchpy-env
	virtualenv couchpy-env --no-site-packages
	bash -c "source couchpy-env/bin/activate ; python ./setup.py develop"

testall :
	cd couchpy/test/; python ./test_client.py
	cd couchpy/test/; python ./test_database.py

bdist_egg :
	python ./setup.py bdist_egg

upload : 
	python ./setup.py bdist_egg register upload --show-response 
	
sdist :
	python ./setup.py sdist

cleanall : clean
	rm -rf couchpy-env

clean :
	rm -rf build;
	rm -rf dist;
	rm -rf CouchPy.egg-info;
	rm -rf CouchPy.egg-info/;
	rm -rf `find ./ -name "*.pyc"`;
	rm -rf `find ./ -name "yacctab.py"`;
	rm -rf `find ./ -name "lextab.py"`;
