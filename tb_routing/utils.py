import itertools as it, operator as op, functools as ft
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


def attr_struct(cls=None, **kws):
	if not cls: return ft.partial(attr_struct, **kws)
	try:
		keys = cls.keys
		del cls.keys
	except AttributeError: pass
	else:
		if isinstance(keys, str): keys = keys.split()
		for k in keys: setattr(cls, k, attr.ib())
	return attr.s(cls, slots=True)

def attr_init(factory=None, **attr_kws):
	factory = attr.Factory(factory) if factory else attr.NOTHING
	return attr.ib(default=factory, **attr_kws)


def coroutine(func):
	@ft.wraps(func)
	def cr_wrapper(*args, **kws):
		cr = func(*args, **kws)
		next(cr)
		return cr
	return cr_wrapper

inf = float('inf')


use_pickle_cache = os.environ.get('TB_PICKLE')
pickle_log = get_logger('pickle')

def pickle_dump(state, name=use_pickle_cache or 'state.pickle'):
	import pickle
	with open(name, 'wb') as dst:
		pickle_log.debug('Pickling data (type: {}) to: {}', state.__class__.__name__, name)
		pickle.dump(state, dst)

def pickle_load(name=use_pickle_cache or 'state.pickle', fail=Fals):
	import pickle
	try:
		with open(name, 'rb') as src:
			pickle_log.debug('Unpickling data from: {}', name)
			return pickle.load(src)
	except OSError as err:
		if fail: raise
