from collections import OrderedDict
import re, pickle, base64, hashlib, time

from . import utils as u


class CacheCondition(Exception): pass
class CacheMissing(CacheCondition): pass
class CacheInvalidated(CacheCondition): pass
class CacheLazySkipped(CacheCondition): pass

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


	def __init__(self, cache_dir, seed, lazy=False, invalidate=None, dep_tree_file=None):
		self.name_fmt = 'v{:02d}.{}.{{}}.cache'.format(self.version, self.seed_hash(seed))
		self.cache_dir = cache_dir
		self.invalidate, self.invalidated = invalidate or list(), set()

		if dep_tree_file and dep_tree_file.exists():
			with dep_tree_file.open() as src: self.dep_tree = self.parse_asciitree(src)
		else: self.dep_tree = dict()

		if lazy:
			item_re = re.compile('^{}$'.format(
				re.escape(self.name_fmt.format('@')).replace(r'\@', r'(.*)') ))
			self.lazy_skip = list( item_re.search(p.name).group(1)
					for p in self.cache_dir.glob(self.name_fmt.format('*')) )
		else: self.lazy_skip = list()

		self.log = u.get_logger('main.cache')


	def check_skip(self, func_id, tree=None, cached_branch=False):
		'''Test if cache loading should be skipped as higher-level data is available.
			Invalidated sub-caches still invalidate whole branch and prevent skipping stuff in it.'''
		if tree is None: tree = self.dep_tree
		if not tree: return # reached leaf without finding func_id, no result

		subtree_checks = set()
		for pat, tree in tree.items():
			match = pat in func_id
			if cached_branch:
				if match: return True # func_id can be skipped
			elif match: return False

			for func_id_sub in self.lazy_skip:
				if pat not in func_id_sub: continue
				if not self.check_valid(func_id_sub): return False # invalidated subtree
				subtree_cached = True
				break
			else: subtree_cached = False

			subtree_checks.add(
				self.check_skip(func_id, tree, subtree_cached) )

		# Make sure result is skipped only if func_id is
		#  1) listed in dep-tree and 2) all matches are in cached branches.
		return True in subtree_checks and False not in subtree_checks


	def check_valid(self, func_id):
		'Test if cache is invalidated by func_id pattern or dep-change.'
		if func_id in self.invalidated: return False
		if any((pat in func_id) for pat in self.invalidate): return False
		return self._check_valid_tree(func_id, self.dep_tree or dict())

	def _check_valid_tree(self, func_id, tree, chk_str=None):
		for pat, tree in tree.items():
			if pat in func_id: # only relevant branch
				return self._check_valid_tree(
					func_id, tree, '\0'.join(self.invalidated) ) is not False
			elif chk_str and pat in chk_str: return False
			subtree_check = self._check_valid_tree(func_id, tree, chk_str=chk_str)
			if subtree_check is not None: return subtree_check


	def serialize(self, data, dst_file): pickle.dump(data, dst_file)
	def unserialize(self, src_file): return pickle.load(src_file)

	def run(self, func, *args, **kws):
		func_id = '.'.join([func.__module__.strip('__'), func.__name__])

		if self.cache_dir:
			cache_file = self.cache_dir / self.name_fmt.format(func_id)
			try:
				if not self.check_valid(func_id): raise CacheInvalidated
				if self.check_skip(func_id):
					self.log.debug('[{}] Lazy-skipped cache-file load: {}', func_id, cache_file.name)
					return CacheLazySkipped()
				if not cache_file.exists(): raise CacheMissing
				with cache_file.open('rb') as src:
					cache_td = time.monotonic()
					data = self.unserialize(src)
					cache_td = time.monotonic() - cache_td
			except CacheInvalidated:
				self.log.debug('[{}] Invalidated cache: {}', func_id, cache_file.name)
			except CacheMissing: pass
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
