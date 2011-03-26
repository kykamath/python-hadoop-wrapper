'''
Created on Feb 25, 2011

@author: kykamath
'''
import sys
from itertools import groupby
from operator import itemgetter

separator = '\t'

class Mapper:
    def iterate_input(self):
        for line in sys.stdin: yield line.strip()
    def write_output(self, key, value, separator=separator): print '%s%s%s' % (str(key), separator, str(value))

class Reducer:
    def __iterate_mapper_output(self, separator=separator):
        for line in sys.stdin: yield line.rstrip().split(separator, 1)
    def iterate_key_values(self):
        for key, values in groupby(self.__iterate_mapper_output(), itemgetter(0)): yield (key, [d[1] for d in values])
    
class Config:
    '''
    /usr/lib/hadoop/bin/hadoop jar /usr/lib/hadoop/contrib/streaming/hadoop-streaming-0.20.2+320.jar 
    -file wordcount_map.py -file wordcount_reduce.py -mapper wordcount_map.py -reducer wordcount_reduce.py -input pg1661.txt -output wc_example -jobconf mapred.reduce.tasks=2
    '''
    def __init__(self, hadoop_dir, streaming_jar, mapper, reducer, input_files, output, jobopts, other_files):
        self.hadoop_dir, self.streaming_jar, self.mapper, self.reducer, self.input_files, self.output, self.jobopts, self.other_files = hadoop_dir, streaming_jar, mapper, reducer, input_files, output, jobopts, other_files
    def getCommand(self):
        command = '%s/bin/hadoop jar %s/contrib/streaming/%s'%(self.hadoop_dir, self.hadoop_dir, self.streaming_jar)
        for f in [self.mapper, self.reducer]: command+=' -file %s '%f
        command+=' -mapper %s -reducer %s '%(self.mapper, self.reducer)
        command+=' -input '.join(['']+self.input_files)
        command+=' -output %s '%self.output
        if self.jobopts: command+=' -jobconf '.join(['']+['%s=%s'%i for i in self.jobopts.iteritems()])
        if self.other_files: command+=' -file '.join(['']+['%s'%i for i in self.other_files])
        return command
