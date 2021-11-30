#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Tue Nov 30 14:55:06 2021

@author: mavroudo
"""
import os,re
if __name__ == "__main__":
    name="testing"
    stream = os.popen(
        """docker run --network followanalyticsexperiments_net fa_preprocess {} {}""".format(name,"indexing 1 0 1 normal 1000 10 20 30"))
    output = stream.read()
    print(output)
    try:
        m = int(re.search("Time taken: ([0-9]*?) ms", output).group(1)) / 1000
        print(m)
        with open("""output/{}.txt""".format(name), "w") as f:
            f.write(str(m))
    except:
        print("nop")