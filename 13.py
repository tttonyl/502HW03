#!/usr/bin/env python2

# To get started with the join, 
# try creating a new directory in HDFS that has both the fwiki data AND the maxmind data.

import mrjob
from mrjob.job import MRJob
import os
from mrjob.step import MRStep
class FwikiMaxmindJoin(MRJob):
    SORT_VALUES = True
    def mapper(self, _, line):
            fields = line.split("\t")
            self.increment_counter("Info","wikipedia",1)
            yield fields[2][:7], 1


    def reducer(self, key, values):
        yield key, sum(values)



    def mapper2(self, key, values):
        yield "term", (key, values)

    # def reducer3_init(self):
    #     self.aaa = []
    def reducer2(self,key, values):
        for v in values:
            # self.aaa.append((key,v))
            # self.aaa.sort()
            yield v

    # def reducer3_init(self):
    #     self.aaa = []

    # def reducer3(self,key, values):
    #     for v in values:
    #         self.aaa.append((key,v))
    #         self.aaa = sorted(self.aaa)

    # SORT_VALUES = True

    # def reducer3_final(self):
    #     for v in self.aaa:
    #         yield v 

    
    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),
            MRStep(mapper=self.mapper2,
                   reducer=self.reducer2)

            # MRStep(reducer_init=self.reducer3_init,
            #        reducer=self.reducer3,
            #        reducer_final=self.reducer3_final)
]


if __name__=="__main__":
    FwikiMaxmindJoin.run()

