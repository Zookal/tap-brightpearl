.DEFAULT_GOAL := test

test:
	pylint tap_brightpearl -d missing-docstring
	nosetests tests/unittests
