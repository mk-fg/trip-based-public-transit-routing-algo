from collections import OrderedDict
import re, pickle, base64, hashlib, time

from . import utils as u, types as t


class CalculationCache:
	'''Wrapper to cache calculation steps to disk.
		Used purely for easier/faster testing of the steps that follow.'''

	version = 1

	@staticmethod
	def seed_hash(val, n=6):
		return base64.urlsafe_b64encode(
			hashlib.sha256(repr(val).encode()).digest() ).decode()[:n]

	@staticmethod
	def parse_asciitree(src_file):
		tree, node_pos = OrderedDict(), dict()
		for line in src_file:
			if line.lstrip().startswith('#') or not line.strip(): continue
			tokens = iter(re.finditer(r'(\|)|(\+-+)|(\S.*$)', line))
			node_parsed = False
			for m in tokens:
				assert not node_parsed, line # only one node per line
				pos, (t_next, t_leaf, val) = m.start(), m.groups()
				if t_next:
					assert pos in node_pos, [line, pos, node_pos]
					continue
				elif t_leaf:
					m = next(tokens)
					val, pos_sub = m.group(3), m.start()
				elif val:# root
					assert not tree, line
					pos_sub = 1
				parent = node_pos[pos] if pos else tree
				node = parent[val] = dict()
				node_parsed, node_pos[pos_sub] = True, node
		return tree

	def __init__(self, cache_dir, seed, invalidate=None, dep_tree_file=None):
		self.cache_dir, self.seed = cache_dir, self.seed_hash(seed)
		self.invalidate, self.invalidated = invalidate or list(), set()
		if dep_tree_file and dep_tree_file.exists():
			with dep_tree_file.open() as src: self.dep_tree = self.parse_asciitree(src)
		else: self.dep_tree = dict()
		self.log = u.get_logger('main.cache')

	def serialize(self, data, dst_file): pickle.dump(data, dst_file)
	def unserialize(self, src_file): return pickle.load(src_file)

	def cache_valid_check(self, func_id, cache_file):
		if func_id in self.invalidated: return False
		if any((pat in func_id) for pat in self.invalidate): return False
		return self._cache_dep_tree_check(func_id, self.dep_tree or dict())

	def _cache_dep_tree_check(self, func_id, tree, chk_str=None):
		for pat, tree in tree.items():
			if pat in func_id:
				return self._cache_dep_tree_check(
					func_id, tree, '\0'.join(self.invalidated) )
			elif chk_str and pat in chk_str: return False
			self._cache_dep_tree_check(func_id, tree, chk_str=chk_str)
		return True

	def run(self, func, *args, **kws):
		func_id = '.'.join([func.__module__.strip('__'), func.__name__])

		if self.cache_dir:
			cache_file = (self.cache_dir / ('.'.join([
				'v{:02d}'.format(self.version), self.seed, func_id ]) + '.cache'))
			if cache_file.exists():
				try:
					if not self.cache_valid_check(func_id, cache_file): raise AssertionError
					with cache_file.open('rb') as src:
						cache_td = time.monotonic()
						data = self.unserialize(src)
						cache_td = time.monotonic() - cache_td
				except AssertionError as err:
					self.log.debug('[{}] Invalidated cache: {}', func_id, cache_file.name)
				except Exception as err:
					self.log.exception( '[{}] Failed to process cache-file, skipping'
						' it: {} - [{}] {}', func_id, cache_file.name, err.__class__.__name__, err )
				else:
					self.log.debug('[{}] Returning cached result (loaded in {:.1f}s)', func_id, cache_td)
					return data
		self.invalidated.add(func_id)

		self.log.debug('[{}] Starting...', func_id)
		func_td = time.monotonic()
		data = func(*args, **kws)
		func_td = time.monotonic() - func_td
		self.log.debug('[{}] Finished in: {:.1f}s', func_id, func_td)

		if self.cache_dir:
			with cache_file.open('wb') as dst:
				cache_td = time.monotonic()
				self.serialize(data, dst)
				cache_td = time.monotonic() - cache_td
				self.log.debug('[{}] Stored cache result (took {:.1f}s)', func_id, cache_td)
		return data
