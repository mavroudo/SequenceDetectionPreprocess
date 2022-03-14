#!/usr/bin/env python
# -*- coding: utf-8 -*-

import matplotlib.pyplot as plt
import pandas as pd
import matplotlib as mpl
#plt.rcParams['font.size'] = '12'
plt.rcParams['hatch.linewidth'] = 1
plt.rcParams['xtick.labelsize'] = 14
plt.rcParams['ytick.labelsize'] = 12

data=pd.read_excel('one_pc_cassandra_times.ods',skiprows=1,usecols="B,C,D,E")
data=data.drop([11],axis=0)
data=data/1000
hatches=['','\\\\','////','--']
colors=['#fde6ee','#fdf5e6','#e6fdf5','#e6eefd']

#Small
small=data.iloc[[0,1,2,3]]
index=["100","1,000","10,000","100,000"]
small.index=index
ax=small.plot.bar(rot=20,align='center',color=colors,width=0.9,edgecolor='black')
for i,t in enumerate(ax.patches):
    t.set(hatch = hatches[i//4], fill=True)
plt.yscale('log')
plt.xlabel('# traces', fontsize=18)
plt.ylabel('Time (s)',fontsize=18)
plt.grid(axis='y')
#plt.legend(loc='upper center', bbox_to_anchor=(0.4, 1.3),fancybox=True, shadow=True, ncol=2)
plt.legend()
plt.ylim([0,80000])
plt.tight_layout()
plt.savefig('preprocess_small.eps',format='eps')

#Medium
medium=data.iloc[[4,5,6,7]]
index=["100","1,000","10,000","100,000"]
medium.index=index
ax=medium.plot.bar(rot=20,color=colors,width=0.9,edgecolor='black')
for i,t in enumerate(ax.patches):
    t.set(hatch = hatches[i//4], fill=True)
plt.yscale('log')
plt.xlabel('# traces', fontsize=18)
plt.ylabel('Time (s)',fontsize=18)
plt.grid(axis='y')
plt.legend()
plt.ylim([0,80000])
plt.tight_layout()
plt.savefig('preprocess_medium.eps',format='eps')

#Large
large=data.iloc[[8,9,10]]
index=["100","1,000","10,000"]
large.index=index
ax=large.plot.bar(rot=20,color=colors,width=0.9,edgecolor='black')
for i,t in enumerate(ax.patches):
    t.set(hatch = hatches[i//3], fill=True)
plt.yscale('log')
plt.xlabel('# traces', fontsize=18)
plt.ylabel('Time (s)',fontsize=18)
plt.grid(axis='y')
plt.legend()
plt.ylim([0,80000])
plt.tight_layout()
plt.savefig('preprocess_large.eps',format='eps')

#Real world
real=data.iloc[[11,12,13]]
index=["bpi_2017","bpi_2018","bpi_2019"]
real.index=index
ax=real.plot.bar(rot=20,color=colors,width=0.9,edgecolor='black')
for i,t in enumerate(ax.patches):
    t.set(hatch = hatches[i//3], fill=True)
plt.xlabel('Dataset',fontsize=18)
plt.ylabel('Time (s)',fontsize=18)
plt.grid(axis='y')
#plt.legend(loc='upper center', bbox_to_anchor=(0.38, 1.4),fancybox=True, shadow=True, ncol=2)
plt.legend()
plt.ylim([0,1200])
plt.tight_layout()
plt.savefig('preprocess_real_world.eps',format='eps')