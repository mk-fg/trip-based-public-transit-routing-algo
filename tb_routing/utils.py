import itertools as it, operator as op, functools as ft
from pathlib import Path
from collections import UserList
import os, sys, logging, datetime, base64
import contextlib, tempfile, stat, warnings

import attr


class LogMessage:
	def __init__(self, fmt, a, k): self.fmt, self.a, self.k = fmt, a, k
	def __str__(self): return self.fmt.format(*self.a, **self.k) if self.a or self.k else self.fmt

class LogStyleAdapter(logging.LoggerAdapter):
	def __init__(self, logger, extra=None):
		super(LogStyleAdapter, self).__init__(logger, extra or {})
	def log(self, level, msg, *args, **kws):
		if not self.isEnabledFor(level): return
		log_kws = {} if 'exc_info' not in kws else dict(exc_info=kws.pop('exc_info'))
		msg, kws = self.process(msg, kws)
		self.logger._log(level, LogMessage(msg, args, kws), (), log_kws)

get_logger = lambda name: LogStyleAdapter(logging.getLogger(name))


def b64(data):
	return base64.urlsafe_b64encode(data).rstrip(b'=').decode()

def get_uid_token(chars=4):
	assert chars * 6 % 8 == 0, chars
	return b64(os.urandom(chars * 6 // 8))

def log_lines(log_func, lines, log_func_last=False):
	if isinstance(lines, str): lines = list(line.rstrip() for line in lines.rstrip().split('\n'))
	uid = get_uid_token()
	for n, line in enumerate(lines, 1):
		if isinstance(line, str): line = '[{}] {}', uid, line
		else: line = ['[{}] {}'.format(uid, line[0])] + list(line[1:])
		if log_func_last and n == len(lines): log_func_last(*line)
		else: log_func(*line)


def attr_struct(cls=None, vals_to_attrs=False, defaults=..., **kws):
	if not cls:
		return ft.partial( attr_struct,
			vals_to_attrs=vals_to_attrs, defaults=defaults, **kws )
	try:
		keys = cls.keys
		del cls.keys
	except AttributeError: keys = list()
	else:
		attr_kws = dict()
		if defaults is not ...: attr_kws['default'] = defaults
		if isinstance(keys, str): keys = keys.split()
		for k in keys: setattr(cls, k, attr.ib(**attr_kws))
	if vals_to_attrs:
		for k, v in vars(cls).items():
			if k.startswith('_') or k in keys or callable(v): continue
			setattr(cls, k, attr.ib(v))
	kws.setdefault('hash', not hasattr(cls, '__hash__'))
	kws.setdefault('slots', True)
	return attr.s(cls, **kws)

def attr_init(factory_or_default=attr.NOTHING, **attr_kws):
	if callable(factory_or_default): factory_or_default = attr.Factory(factory_or_default)
	return attr.ib(default=factory_or_default, **attr_kws)

attr_init_id = lambda: attr_init(lambda seq=iter(range(2**40)): next(seq))


p = ft.partial(print, flush=True)

def die(code=0):
	sys.stdout.close()
	sys.stderr.close()
	os._exit(code)

def coroutine(func):
	@ft.wraps(func)
	def cr_wrapper(*args, **kws):
		cr = func(*args, **kws)
		next(cr)
		return cr
	return cr_wrapper

def get_any(d, *keys):
	for k in keys:
		try: return d[k]
		except KeyError: pass

def init_if_none(v, default):
	if v is None: v = default() if callable(default) else v
	return v

def same_type_and_id(v1, v2):
	return type(v1) is type(v2) and v1.id == v2.id

@contextlib.contextmanager
def supress_warnings():
	'''Similar to warnings.catch_warnings(),
		but does not miss stuff like DeprecationWarning.'''
	filters_bak = warnings.filters
	warnings.filterwarnings('ignore')
	try: yield
	finally: warnings.filters[:] = filters_bak

inf = float('inf')

def max(iterable, default=..., _max=max, **kws):
	try: return _max(iterable, **kws)
	except ValueError:
		if default is ...: raise
		return default

def min(iterable, default=..., _min=min, **kws):
	try: return _min(iterable, **kws)
	except ValueError:
		if default is ...: raise
		return default


@contextlib.contextmanager
def safe_replacement(path, *open_args, mode=None, **open_kws):
	path = str(path)
	if mode is None:
		try: mode = stat.S_IMODE(os.lstat(path).st_mode)
		except OSError: pass
	open_kws.update( delete=False,
		dir=os.path.dirname(path), prefix=os.path.basename(path)+'.' )
	if not open_args: open_kws['mode'] = 'w'
	with tempfile.NamedTemporaryFile(*open_args, **open_kws) as tmp:
		try:
			if mode is not None: os.fchmod(tmp.fileno(), mode)
			yield tmp
			if not tmp.closed: tmp.flush()
			os.rename(tmp.name, path)
		finally:
			try: os.unlink(tmp.name)
			except OSError: pass


use_pickle_cache = os.environ.get('TB_PICKLE')
pickle_log = get_logger('pickle')

def pickle_dump(state, name=use_pickle_cache or 'state.pickle'):
	import pickle
	with safe_replacement(name, 'wb') as dst:
		pickle_log.debug('Pickling data (type={}) to: {}', state.__class__.__name__, name)
		pickle.dump(state, dst)

def pickle_load(name=use_pickle_cache or 'state.pickle', fail=False):
	import pickle
	try:
		with open(str(name), 'rb') as src:
			pickle_log.debug('Unpickling data from: {}', name)
			return pickle.load(src)
	except Exception as err:
		if fail: raise


def dts_parse(dts_str):
	if ':' not in dts_str: return float(dts_str)
	dts_vals = dts_str.split(':')
	if len(dts_vals) == 2: dts_vals.append('00')
	assert len(dts_vals) == 3, dts_vals
	return sum(int(n)*k for k, n in zip([3600, 60, 1], dts_vals))

def dts_format(dts):
	dts_days, dts = divmod(int(dts), 24 * 3600)
	dts = str(datetime.time(dts // 3600, (dts % 3600) // 60, dts % 60, dts % 1))
	if dts_days: dts = '{}+{}'.format(dts_days, dts)
	return dts
