#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Sat Aug 28 11:09:07 2021

@author: mavroudo
"""

import subprocess
import os

bash_cmd = """docker run \
--mount type=bind,source={}/input,target=/app/input \
--mount type=bind,source={}/output/,target=/app/output \
preprocess:1.0 {} {} {}"""

for mode in ["norma","signature","setcontainment"]:
    for f in os.listdir("input") :
        if ".xes" in f:
            print(f,mode)
            process = subprocess.Popen(bash_cmd.format(os.getcwd(),os.getcwd(),"normal","input/"+f,"indexing").split(), stdout=subprocess.PIPE)
            output,error = process.communicate()

