clean:
	rm -rf build/ dist/ libtasks.egg-info/
	find . -name "*pyc" -exec rm -rf {} \;