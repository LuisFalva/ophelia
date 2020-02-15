# -*- coding: utf-8 -*-
"""
Created on Thu Feb  6 11:47:19 2020

@author: s4661708
"""

import pandas as pd
import numpy as np

import pyspark
from pyspark.sql import SparkSession
from pyspark.sql.functions import lit, col, array, explode, struct



spark = SparkSession.builder.appName('Rank_Mean').getOrCreate()
sc = spark.sparkContext

num_of_assets = 10
monthly_data = pd.read_csv("data1.csv")

benchmark_month = monthly_data.loc[0:monthly_data.shape[0], monthly_data.columns[1]]

pct_benchmark_month = benchmark_month.pct_change(1)

pct_benchmark_month_array = np.array(pct_benchmark_month)

investment_universe_month = monthly_data.loc[0:monthly_data.shape[0],monthly_data.columns[2:monthly_data.shape[1]]]

pct_investment_month = investment_universe_month.pct_change(1)

pct_investment_month_array = np.array(pct_investment_month)


up_month = np.zeros((pct_benchmark_month_array.shape[0]+1, 1))
down_month = np.zeros((pct_benchmark_month_array.shape[0]+1, 1))
up_move = np.zeros((pct_benchmark_month_array.shape[0]+1, pct_investment_month_array.shape[1]))
down_move = np.zeros((pct_benchmark_month_array.shape[0]+1, pct_investment_month_array.shape[1]))


size_benchmark_matrix = pct_benchmark_month_array.shape[0]
for i in range (1, size_benchmark_matrix):
    if pct_benchmark_month_array[i] > 0:
        up_month[i] = pct_benchmark_month_array[i]
        up_move[i] = pct_investment_month_array[i, 0:pct_investment_month_array.shape[1]]
    else:
        down_month[i] = pct_benchmark_month_array[i]
        down_move[i] = pct_investment_month_array[i, 0:pct_investment_month_array.shape[1]]
        


## calculamos los vectores 'peor más alto' y 'mejor más alto'

np.seterr(divide='ignore', invalid='ignore')
greater_worse = down_move / down_month
greater_better = (up_move / up_month) * float(-1.0)



greater_worse_df = pd.DataFrame(data=greater_worse).dropna()
greater_better_df = pd.DataFrame(data=greater_better).dropna()



median_down = greater_worse_df.expanding().median()
median_up = greater_better_df.expanding().median()

down_transpose = median_down.T
up_transpose = median_up.T


ranked_down = down_transpose.rank()
transpose_ranked_down = ranked_down.T

ranked_up = up_transpose.rank()
transpose_ranked_up = ranked_up.T


worse_better_df = pd.concat([transpose_ranked_up, transpose_ranked_down]).sort_index()

worse_better_df['closing_id'] = range(1, len(worse_better_df) + 1)

worse_better = spark.createDataFrame(worse_better_df)


def shape_long_format(dataframe, pivot_col):
    
    columns, data_type = zip(*((c, t) for (c, t) in dataframe.dtypes if c not in pivot_col))
    assert len(set(data_type)) == 1, "Columns not the same data type..."
    
    column_explode = explode(array([
        struct(lit(c).alias("asset_id"), col(c).alias("top_rank")) for c in columns
    ])).alias("column_explode")

    return dataframe.select(pivot_col + [column_explode]).select(pivot_col + ["column_explode.asset_id", "column_explode.top_rank"])



asset_ranking_df = shape_long_format(worse_better, ["closing_id"]).where(col("top_rank") <= 10).orderBy("closing_id", "top_rank")
asset_ranking_df.show(100000)

pandas_df = asset_ranking_df.select("*").toPandas()
pandas_df = pandas_df.astype({'asset_id':'int32'})
newselect = pandas_df[["closing_id","asset_id"]]

indexed = np.zeros((investment_universe_month.shape[0],num_of_assets))

for q in range(1,investment_universe_month.shape[0]):
    selection = newselect.loc[pandas_df["closing_id"]==q]
    newselect_transpose = selection.T
    newpdf = newselect_transpose['asset_id':].head()
    indexed[q] = newpdf

indexed = indexed[1:investment_universe_month.shape[0]+1]

index_row = indexed.astype(np.int64)
newrow = np.zeros((1,num_of_assets))
index_row = np.vstack([index_row,newrow])
portfolio = np.zeros((investment_universe_month.shape[0],num_of_assets))

for r in range(0,investment_universe_month.shape[0]-1):
    s = r+1
    columns = index_row[s]
    print(columns)
    portfolio[s] = pct_investment_month_array[s,[columns]]
    
    
    
performance = np.dot(portfolio,(1/num_of_assets))
returns = np.zeros((investment_universe_month.shape[0]-1,1))

for x in range (1,investment_universe_month.shape[0]):
    returns[x-1] = sum(performance[x])

    



