#!/usr/bin/env python
import argparse
import sys
import csv
from collections import defaultdict

if not 'reduce' in dir(__builtins__):
    from functools import reduce

class AttrDict(dict):
    def __init__(self, *args, **kwargs):
        super(AttrDict, self).__init__(*args, **kwargs)
        self.__dict__ = self

rdd=lambda: defaultdict(rdd)
udd=lambda d: {k:udd(v) if isinstance(v,defaultdict) else v for k,v in d.items()}
ufsdd=lambda d: [AttrDict({k1:v1 if isinstance(v1,str) else ufsdd(v1) for k1,v1 in v.items()}) for k,v in d.items()]

def process(cdef,row,level=rdd()):
    leafs=[]
    nlevels={}
    for k,v in cdef.items():
        if isinstance(v,int):
            leafs.append((k,row[v]))
        else:
            nlevels[k]=v
    level[frozenset(leafs)].update(dict(leafs))
    for nlevelk,nlevelv in nlevels.items():
        process(nlevelv,row,level[frozenset(leafs)][nlevelk])

if __name__ == '__main__':
    parser=argparse.ArgumentParser()
    parser.add_argument('-d','--delimiter',default='.')
    parser.add_argument('-o','--output',type=argparse.FileType('w'),default=sys.stdout)
    parser.add_argument('template',type=argparse.FileType('r'))
    parser.add_argument('csv',nargs='?',type=argparse.FileType('r'),default=sys.stdin)
    args=parser.parse_args(sys.argv[1:])

    reader=csv.reader(args.csv)
    header = next(reader)

    definition=rdd()
    for ix,val in enumerate(header):
        spl=val.split(args.delimiter)
        path=spl[:-1]
        leaf=spl[-1]
        reduce(lambda o,v: o[v],path,definition)[leaf]=ix
    definition=udd(definition)

    objects=rdd()
    for row in reader:
        process(definition,row,objects)
    objects=ufsdd(objects)

    from pprint import pprint; pprint(objects)
