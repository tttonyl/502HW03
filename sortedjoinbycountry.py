#!/usr/bin/env python2

# To get started with the join, 
# try creating a new directory in HDFS that has both the fwiki data AND the maxmind data.

import mrjob
from mrjob.job import MRJob
from weblog import Weblog       # imports class defined in weblog.py
import os
import heapq
from mrjob.step import MRStep

class FwikiMaxmindJoin(MRJob):
    SORT_VALUES = True
    def mapper(self, _, line):
        # Is this a weblog file, or a MaxMind GeoLite2 file?
        filename = mrjob.compat.jobconf_from_env("map.input.file")
        if "top1000ips_to_country.txt" in filename:
            fields = line.split("\t")
            self.increment_counter("Info","top1000_ips_to_country Count",1)

            # Handle as a GeoLite2 file
            #
            yield fields[0], ("country", fields)
        else:
            log = Weblog(line)
            logfields = (log.ipaddr,log.url,log.date,log.time,log.datetime,log.wikipage())
            # Handle as a weblog file
            self.increment_counter("Info","weblog Count",1)
            yield logfields[0], ("ip",logfields)


    def reducer(self, key, values):
        country = None
        for v in values:
            if len(v)!=2:
                self.increment_counter("Warn","Invalid Join",1)
                continue
            if v[0]=="country":
                country = v[1]
                continue
            if v[0]=="ip":
                ip = v[1]
                if country:
                    assert key == country[0]
                    assert key == ip[0]
                    yield ip[0], (country[1],ip)



    def mapper2(self, key, values):
        yield values[0], 1


    def reducer2(self, key, values):
        yield key, sum(values)

    def mapper3(self, key, values):
        yield "country", (key, values)

    # def reducer3_init(self):
    #     self.aaa = []
    def reducer3(self,key, values):
        for v in values:
            # self.aaa.append((key,v))
            # self.aaa.sort()
            yield v

    # def reducer3_final(self):
    #     for v in self.aaa:
    #         yield v 


    def steps(self):
        return [
            MRStep(mapper=self.mapper,
                   reducer=self.reducer),

            MRStep(mapper=self.mapper2,
                   reducer=self.reducer2),

            MRStep( mapper=self.mapper3,
                    # reducer_init=self.reducer3_init,
                   reducer=self.reducer3)
                   # reducer_final=self.reducer3_final)
]


if __name__=="__main__":
    FwikiMaxmindJoin.run()

