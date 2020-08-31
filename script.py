#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Created on Fri Aug 28 12:29:29 2020

@author: mavroudo
"""

import os
import re
stream = os.popen("""/opt/spark/bin/spark-submit --master local[*]  --conf spark.cassandra.connection.host=rabbit.csd.auth.gr --conf spark.cassandra.output.consistency.level=ONE target/scala-2.11/Sequence\ Detection-assembly-0.1.jar 'ten$2.withTimestamp' indexing 0 0 0""")
output = stream.read()

re.match("Time taken: ?")

print(output)



