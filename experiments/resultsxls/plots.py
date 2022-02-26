#!/usr/bin/env python
# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
import pandas as pd
plt.rcParams['font.size'] = '12'

data=pd.read_excel('one_pc_cassandra_times.ods',skiprows=1,usecols="B,C,D,E")
data=data.drop([11],axis=0)
data=data/1000

#Small
small=data.iloc[[0,1,2,3]]
index=["100","1,000","10,000","100,000"]
small.index=index
small.plot.bar(rot=30)
plt.yscale('log')
plt.xlabel('# traces')
plt.ylabel('Time (s)')
plt.grid(axis='y')
plt.legend(loc='upper center', bbox_to_anchor=(0.4, 1.3),fancybox=True, shadow=True, ncol=2)
plt.tight_layout()
plt.savefig('preprocess_small.eps',format='eps')

#Medium
medium=data.iloc[[4,5,6,7]]
index=["100","1,000","10,000","100,000"]
medium.index=index
medium.plot.bar(rot=30)
plt.yscale('log')
plt.xlabel('# traces')
plt.ylabel('Time (s)')
plt.grid(axis='y')
plt.legend(loc='upper center', bbox_to_anchor=(0.4, 1.3),fancybox=True, shadow=True, ncol=2)
plt.tight_layout()
plt.savefig('preprocess_medium.eps',format='eps')

#Large
large=data.iloc[[8,9,10]]
index=["100","1,000","10,000"]
large.index=index
large.plot.bar(rot=30)
plt.yscale('log')
plt.xlabel('# traces')
plt.ylabel('Time (s)')
plt.grid(axis='y')
plt.legend(loc='upper center', bbox_to_anchor=(0.38, 1.32),fancybox=True, shadow=True, ncol=2)
plt.tight_layout()
plt.savefig('preprocess_large.eps',format='eps')

#Real world
real=data.iloc[[11,12,13]]
index=["bpi_2017","bpi_2018","bpi_2019"]
real.index=index
real.plot.bar(rot=30)
plt.xlabel('Dataset')
plt.ylabel('Time (s)')
plt.grid(axis='y')
plt.legend(loc='upper center', bbox_to_anchor=(0.38, 1.4),fancybox=True, shadow=True, ncol=2)
plt.tight_layout()
plt.savefig('preprocess_real_world.eps',format='eps')