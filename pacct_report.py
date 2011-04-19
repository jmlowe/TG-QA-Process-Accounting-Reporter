import gzip,bz2
import struct
import time
try:
  import json
except ImportError:
  import simplejson as json
try:
    from collections import defaultdict
except:
    class defaultdict(dict):
        def __init__(self, default_factory=None, *a, **kw):
            if (default_factory is not None and
                not hasattr(default_factory, '__call__')):
                raise TypeError('first argument must be callable')
            dict.__init__(self, *a, **kw)
            self.default_factory = default_factory
        def __getitem__(self, key):
            try:
                return dict.__getitem__(self, key)
            except KeyError:
                return self.__missing__(key)
        def __missing__(self, key):
            if self.default_factory is None:
                raise KeyError(key)
            self[key] = value = self.default_factory()
            return value
        def __reduce__(self):
            if self.default_factory is None:
                args = tuple()
            else:
                args = self.default_factory,
            return type(self), args, None, None, self.items()
        def copy(self):
            return self.__copy__()
        def __copy__(self):
            return type(self)(self.default_factory, self)
        def __deepcopy__(self, memo):
            import copy
            return type(self)(self.default_factory,
                              copy.deepcopy(self.items()))
        def __repr__(self):
            return 'defaultdict(%s, %s)' % (self.default_factory,
                                            dict.__repr__(self))


import sys
#from collections import defaultdict
user_exclude,cmd_include = json.load(open('pacct_lists.json','r'))
record_size = struct.calcsize('B3HI9HI17s2xII')
colnames = ('flags','uid','gid','tty','btime','utime','stime','etime','mem','io','rw','minflt','majflt','swaps','exitcode','comm','uid32','gid32')

if 'Struct' in dir(struct):
  record_struct = struct.Struct('B3HI9HI17s2xII')
else:
  class record_struct:
    @classmethod
    def unpack(self,record):
      return struct.unpack('B3HI9HI17s2xII',record)

def comp_t_2_double(c_num):
   in_ = c_num & 017777
   c_num >>= 13
   while c_num:
     c_num += -1
     in_ <<= 3
   return float(in_)

def gen_cat(sources):
  for s in sources:
    item = s.read(record_size)
    while item:
      yield item
      item = s.read(record_size)
def gen_open(filenames):
  for name in filenames:
    if name.endswith(".gz"):
      yield gzip.open(name)
    elif name.endswith(".bz2"):
      yield bz2.BZ2File(name)
    else:
      yield open(name)
def field_map(dictseq,name,func, dep_name= None):
  if dep_name == None:
    dep_name = name
  for d in dictseq:
    d[dep_name] = func(d[name])
    yield d

def records(filelist):
  files = gen_open(filelist)
  recs = gen_cat(files)
  record = (dict(zip(colnames,record_struct.unpack(r))) for r in recs)
  record = field_map(record,"comm",lambda x: x.replace('\0',''))
  record = field_map(record, "utime", comp_t_2_double)
  record = field_map(record, "stime", comp_t_2_double)
  record = field_map(record, "etime", comp_t_2_double)
  record = field_map(record, "mem", comp_t_2_double)
  record = field_map(record, "io", comp_t_2_double)
  record = field_map(record, "rw", comp_t_2_double)
  record = field_map(record, "minflt", comp_t_2_double)
  record = field_map(record, "majflt", comp_t_2_double)
  record = field_map(record, "swaps", comp_t_2_double)
  return record

if __name__ == "__main__":
  comm_dict = defaultdict(lambda: defaultdict(int))
  filelist = sys.argv[1:]
  earliestcmd, latestcmd = (time.time(),0)
  for r in (r for r in records(filelist) if r['comm'] in cmd_include and not r['uid'] in user_exclude):
#    print r
    if r['btime'] > latestcmd:
       latestcmd = r['btime']
    elif r['btime'] < earliestcmd:
       earliestcmd = r['btime']
    comm_dict[r["comm"]][r["uid"]] += 1
   
  #  if i >= 10: break
  print json.dumps([{"begin":earliestcmd,"end":latestcmd},comm_dict]) 
