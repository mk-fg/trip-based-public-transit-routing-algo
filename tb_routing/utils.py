import itertools as it, operator as op, functools as ft
from pathlib import Path
from collections import UserList
import os, sys, logging

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


def attr_struct(cls=None, vals_to_attrs=False, **kws):
	if not cls: return ft.partial(attr_struct, vals_to_attrs=vals_to_attrs, **kws)
	try:
		keys = cls.keys
		del cls.keys
	except AttributeError: keys = list()
	else:
		if isinstance(keys, str): keys = keys.split()
		for k in keys: setattr(cls, k, attr.ib())
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

inf = float('inf')


use_pickle_cache = os.environ.get('TB_PICKLE')
pickle_log = get_logger('pickle')

def pickle_dump(state, name=use_pickle_cache or 'state.pickle'):
	import pickle
	with open(str(name), 'wb') as dst:
		pickle_log.debug('Pickling data (type: {}) to: {}', state.__class__.__name__, name)
		pickle.dump(state, dst)

def pickle_load(name=use_pickle_cache or 'state.pickle', fail=False):
	import pickle
	try:
		with open(str(name), 'rb') as src:
			pickle_log.debug('Unpickling data from: {}', name)
			return pickle.load(src)
	except Exception as err:
		if fail: raise


class IDList(UserList):
	'''Easily-mutable list with element
		values compared by their id() instead of equality.'''
	def index(self, v):
		for n, v2 in enumerate(self.data):
			if v is v2: return n
		raise ValueError(v)
	def remove(self, v): self.data.pop(self.index(v))
	def __contains__(self, v):
		try: self.index(v)
		except ValueError: return False
		else: return True
	def __iter__(self): return iter(list(self.data))
