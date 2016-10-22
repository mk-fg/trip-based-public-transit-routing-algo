from . import gtfs_shizuoka
from . import simple


def load_tests(loader, tests, pattern):
	# XXX: because unittest in pypy3/3.3 #  doesn't have subTest ctx yet
	for mod in gtfs_shizuoka, simple:
		tests.addTests(mod.load_tests(loader, tests, pattern))
	return tests
