#!/usr/bin/python
'''
Created on Feb 25, 2011

@author: kykamath
'''
from settings import Settings
from map_reduce_hadoop import Mapper, Config

def mapper():
    mapper = Mapper()
    for line in mapper.iterate_input(): 
        for term in line.split(): mapper.write_output(term, 1)

def config():
    mapper, reducer = 'mapper_demo.py', 'reducer_demo.py'
    input_files = ['pg1661.txt']
    output = 'wc_example'
    jobopts = {
               'mapred.reduce.tasks': 2
               }
    other_files = ['map_reduce_hadoop.py', 'settings.py']
    print Config(Settings.hadoop_folder, Settings.hadoop_streaming_jar, mapper, reducer, input_files, output, jobopts, other_files).getCommand()

if __name__ == '__main__':
    config()
#    mapper()
