# -*- coding: utf-8 -*-
"""
Created on Tue Jan 21 09:55:19 2020

@author: s4661708
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
from pandas import Series, DataFrame
from pandas.tseries.offsets import Day, MonthEnd
import sys
import matplotlib.pyplot as plt
from scipy.stats import rankdata
from scipy.stats import stats
from scipy.optimize import minimize
from mpl_toolkits.mplot3d import Axes3D
from matplotlib import cm
import matplotlib
from pylab import *
from matplotlib.ticker import LinearLocator, FormatStrFormatter
from numpy import *
import seaborn as sns


monthly = pd.read_excel("data.xlsx", "Hoja3")
daily = pd.read_excel("data.xlsx", "Hoja4")
bmkm = monthly.loc[0:monthly.shape[0],monthly.columns[1]]
bmkd = daily.loc[0:daily.shape[0],daily.columns[1]]
bmkm = bmkm.pct_change(1)
bmkd = bmkd.pct_change(1)
bmkm = np.array(bmkm)
bmkd = np.array(bmkd)


invunim = monthly.loc[0:monthly.shape[0],monthly.columns[2:monthly.shape[1]]]
invunid = daily.loc[0:daily.shape[0],daily.columns[2:daily.shape[1]]]
invunim = invunim.pct_change(1)
invunid = invunid.pct_change(1)
invunim = np.array(invunim)
invunid = np.array(invunid)

upm = np.zeros((bmkm.shape[0]+1,1))
downm = np.zeros((bmkm.shape[0]+1,1))
upmove = np.zeros((bmkm.shape[0]+1, invunim.shape[1]))
downmove = np.zeros((bmkm.shape[0]+1, invunim.shape[1]))

for i in range (1,bmkm.shape[0]):
    if bmkm[i] > 0:
        upm[i] = bmkm[i]
        upmove[i] = invunim[i,0:invunim.shape[1]]
    else:
        downm[i] = bmkm[i]
        downmove[i] = invunim[i,0:invunim.shape[1]]

np.seterr(divide='ignore', invalid='ignore')
downcapt = downmove/downm # THE GREATER THE WORSE
upcapt = upmove/upm*-1 #THE GREATER THE BETTER
dfup = pd.DataFrame(data=upcapt) 
dfup = dfup.dropna()
dfdown = pd.DataFrame(data=downcapt)
dfdown = dfdown.dropna()
#### mediana acumulada



mediandown = dfdown.expanding().median()
medianup = dfup.expanding().median()

downtranspose = transpose(mediandown)
uptranspose = transpose(medianup)

indexdown = np.zeros((69,10))
for x in range(0,69):
    indexdown = mediandown.nlargest(10,x)


rankeddown = downtranspose.rank()
rankeddown = transpose(rankeddown)
rankedup = uptranspose.rank()
rankedup = transpose(rankedup)


#est_window = 24
#modelport = np.zeros((bmkm.shape[0]+1,invunim.shape[1]))
#model_perf = np.zeros((bmkm.shape[0]+1,1))


#choose_matrix = []
#for j in range (0,bmkm.shape[0]):
#    #print(j)
#    choose_matrix.append(j)
#    if bmkm[i] > 0:
#        choose_matrix[j] = rankedup[j]
#    else:
#        choose_matrix[j] = rankeddown[j]


ranked = [rankedup, rankeddown]
finalmat = pd.concat(ranked)

finalmat = finalmat.sort_index() # llegar aqui importante
#finalmat = transpose(finalmat)


#for z in range (1,70):
#    month = finalmat.sort_values(by=[z])
#    month = finalmat.nsmallest(10,[z])
#    for a in range (0,70):
#        print(a)
##    month.append(z)
#    month[a,z] = finalmat.nlargest(10,[z])
##    print(month[z])
##    month[z] = month[z].sort()
#    
     

#month = transpose(month)









