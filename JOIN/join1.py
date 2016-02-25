#!/usr/bin/env python2

# To get started with the join, 
# try creating a new directory in HDFS that has both the fwiki data AND the maxmind data.

import mrjob
from mrjob.job import MRJob
from weblog import Weblog       # imports class defined in weblog.py
import os
class FwikiMaxmindJoin(MRJob):
    def mapper(self, _, line):
        # Is this a weblog file, or a MaxMind GeoLite2 file?
        filename = mrjob.compat.jobconf_from_env("map.input.file")
        if "top1000ips_to_country.txt" in filename:
            fields = line.split("\t")
            self.increment_counter("Info","top1000ips_to_country Count",1)

            # Handle as a GeoLite2 file
            #
            yield fields[0], ("country", fields)
        else:
            log = Weblog(line)
            # Handle as a weblog file
            self.increment_counter("Info","weblog Count",1)
            yield log.ipaddr, ("ip",line)


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
                    yield country[1],ip




if __name__=="__main__":
    FwikiMaxmindJoin.run()
