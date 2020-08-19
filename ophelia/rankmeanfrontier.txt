def get_ret_vol_sr(weights):
    weights = np.array(weights)
    ret = np.sum(mean_returns * weights) * 12
    vol = np.sqrt(np.dot(weights.T, np.dot(cov, weights))) * np.sqrt(12)
    sr = ret/vol
    return np.array([ret, vol, sr])

def neg_sharpe(weights):
# the number 2 is the sharpe ratio index from the get_ret_vol_sr
    return get_ret_vol_sr(weights)[2] * -1

def check_sum(weights):
    #return 0 if sum of the weights is 1
    return np.sum(weights)-1
cons = ({'type':'eq', 'fun':check_sum})
bounds = ((0,1),(0,1),(0,1),(0,1),(0,1),(0,1),(0,1),(0,1),(0,1),(0,1))
init_guess = [.1,.1,.1,.1,.1,.1,.1,.1,.1,.1]
opt_results = sco.minimize(neg_sharpe, init_guess,method='SLSQP', bounds=bounds, constraints=cons)
print(opt_results)

get_ret_vol_sr(opt_results.x)


frontier_y = np.linspace(0.140,0.27,200)
frontier_x = []


def minimize_volatility(weights):
    return get_ret_vol_sr(weights)[1]


for possible_return in frontier_y:
    cons = ({'type':'eq', 'fun':check_sum},
            {'type':'eq', 'fun': lambda w: get_ret_vol_sr(w)[0] - possible_return})
    
    result = sco.minimize(minimize_volatility,init_guess,method='SLSQP', bounds=bounds, constraints=cons)
    frontier_x.append(result['fun'])
    
vol_arr = results_frame.stdev
ret_arr = results_frame.ret
sharpe_arr = results_frame.sharpe
# cmap='RdYlBu' 
   
plt.figure(figsize=(12,8))
plt.scatter(vol_arr, ret_arr, c=sharpe_arr, cmap='RdYlBu')
plt.colorbar(label='Sharpe Ratio')
plt.xlabel('Volatility')
plt.ylabel('Return')
plt.plot(frontier_x,frontier_y, 'r--', linewidth=3)
# plt.savefig('cover.png')
plt.show()


