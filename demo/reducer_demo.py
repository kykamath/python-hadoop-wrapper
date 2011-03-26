#!/usr/bin/python
'''
Created on Feb 25, 2011

@author: kykamath
'''
from map_reduce_hadoop import Reducer

def reducer():
    reducer = Reducer()
    for key, values in reducer.iterate_key_values(): print key, sum([int(v) for v in values])

if __name__ == '__main__':
    reducer()