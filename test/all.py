import unittest

from . import gtfs_shizuoka
from . import simple


# XXX: because unittest in pypy3/3.3 doesn't have subTest ctx yet

def load_tests(loader=None, tests=None, pattern=None):
	if not tests: tests = unittest.TestSuite()
	for mod in gtfs_shizuoka, simple:
		tests.addTests(mod.load_tests(loader, tests, pattern))
	return tests

class SpecificTestCasePicker:
	def __init__(self): self.suite = load_tests()
	def __getattr__(self, k):
		for test in self.suite:
			if test._testMethodName == k: return lambda: test
		raise AttributeError('No such test case: {}'.format(k))
case = SpecificTestCasePicker()
