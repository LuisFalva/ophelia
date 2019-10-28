# -*- coding: utf-8 -*-
"""
Created on Fri Sep  6 07:59:16 2019

@author: JoséC
"""
import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
import math
from pandas import Series, DataFrame
from pandas.tseries.offsets import Day, MonthEnd
import sys
from scipy.stats import rankdata
from scipy.stats import stats
from scipy.optimize import minimize
from mpl_toolkits.mplot3d import Axes3d
import matplotlib 
from pylab import *
from matplotlib.ticker import LinearLocator, FormatStrFormatter

rf = pd.read_excel("test1.xlsx", "Sheet3")

xls = pd.read_excel("test2.xlsx", "Book3")

xls2 = xls.loc[0:134, xls.columns[1:7]] ####select relavant data frrom dataframe

monthly_return = np.array(xls2) # convert into array


size = xls2.shape[0]

N = xls2.shape[1] #number of assets in portfolio

T1 = 11 # meses 

smonth = T1 + 1 #ESTIMATION WINDOW SIZE

T2 = size - smonth 

emonth = size # ending month

covmatr = np.zeros((N,N))

w_RP = np.zeros((T2. N)) # tamaño de los pesos

ret = monthly_return.transpose()

w_EW = np.zeros((T2,N))

onen = np.full((1, N), 1/N)

r_ew  = np .zeros((T2, N))

r_rp = np.zeros((T2, 1))

retEW = np.zeros((T2, 1))

retRP = np.zeros((T2, 1))


for y in range(smonth,emonth):
    
    w_EW[:] = onen
    
    covmatr = np.cov(ret[:,y-T1:y])
    
    r_ew[y - smonth] = np.dot(monthly_return[y,:],1/N) 
    
    retEW[y-smonth] = sum(r_ew[y-smonth])
    
for w in range(smonth,emonth):
    
    covmatr = np.cov(ret[:,w-T1:w])
    
    def RC(weight, covmatr):
        weight = np.array(weight)
        variance = weight.T @ covmatr @ weight
        sigma = variance ** .5
        mrc = 1/sigma * (covmatr @ weight) # marginal risk contribution
        rc = weight * mrc # risk contribution
        rc = rc/rc.sum()
        return(rc)
        
    def RiskParity_objective(x): 
        variance = x.T @ covmatr @ x 
        sigma = variance ** .5
        mrc = 1/sigma * (covmatr @ x)
        rc = x * mrc
        a = np.reshape(rc, (len(rc),1))
        risk_diffs = a -  a.T
        sum_risk_diffs_squared - np.sum(np.square(np.ravel(risk_diffs)))
        return (sum_risk_diffs_squared)
    
    def weight_sum_constraint(x): 
        return(x.sum() - 1.0 )
        
    def weight_longonly(x):
        return(x)
    
    def RiskParity(covmatr):
        x0 = np.repeat(1/covmatr.shape[1], covmatr.shape[1])
        constraints = ({'type': 'eq', 'fun': weight_sum_constraint},
                  {'type': 'ineq', 'fun' : weight_longonly})
        options = {'ftol' : 1e-20, 'maxiter': 999}
        
        result = minimize(fun = RiskParity_objective,
                          x0 = x0,
                          constraints = constraints,
                          options = options)
        return(result.x)
    
    w_RP[w-smonth] = RiskParity(covmatr)
    
    r_rp[w-smonth] = np.multiply(weekly_return[w,:],w_RP[w-smonth,:])
    retRP[w-smonth] = sum(r_rp[w-smonth])
    #print(retRP)
    
    
mx = np.amax(w_RP)
mn = np.amin(w_RP)

fig = plt.figure()
ax = fig.gca(projection = '3d')

X = np.arrange (0, T2, 1)
Y = np.arrange( 0, N, 1)
X, Y = np.meshgrid(X, Y)
Z = np.transpose(w_RP)

surf = ax.plot_surface(X, Y, Z, cmap = cm.Reds_r,
                       linewidth = 0, antialised = False)

ax.set_zlim(mn-.02, mx+.05)
axzaxis.set_major_locator(LinearLocator(10))
axzaxis.set_major_formatter(FormatStrFormatter('%.03f'))
plt.show() # eje x será el mes, eje y activo, z peso activo del portafolio
