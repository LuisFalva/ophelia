# [Ophilea](https://luisfalva.github.io/ophilea/)

*Ophelia* to be pronounced as `Ophilea` (Hamlet's beloved beautiful woman... and is the name of the package too) she's known because of her madness and immortal love for Hamlet, but the full Shakespeare's mastery piece does not favor to her magnificent character.
Ophilea is the epitome of goodness, the brightness and the elegance of simplicity.

# Motivations 🚀

As Data Scientist or Data Analyst you don't really want to waist too much time guessing how may use PySpark's API, sometimes we just want a prompt answer instead of a full nice code. With the intent of that, this project
aims to help reducing the complexity of the analytical lifecycle for everyone who use PySpark frequently.

Now is the turn of a new smart and very extravaganza `Ophilea` serving to optimize the learning curve with PySpark's must common functionality features such as:
- Build PySpark *ML & Mllib* pipelines in a simplified replicable and secure way
- Embedded optimized techniques helping users struggling with *data skewness problems*
- Easy to use and build your own models and data mining pipelines with PySpark using *Ophelia spark wrappers*
- Security and simplified usage for exploring new *PySpark features* for data mining replicating the most commonly used functionality in libraries such as Pandas and Numpy
- Simple *Pythonic* syntax: *'Not too fancy things to do the hard work'*
- Utility for *RDD level pre-processing* data in a simple manner
- Time series treatment and portfolio optimization with different techniques based on Portfolio Theory such as Risk Parity, Efficient Frontier, Clustering by Sortion's ratio and Sharpe's ratio, among others

# Getting Started:

### Requirements 📜

Before starting, you'll need to have installed pyspark >= 3.0.x, pandas >= 1.1.3, numpy >= 1.19.1, dask >= 2.30.x, scikit-learn >= 0.23.x 
Additionally, if you want to use the Ophilea API, you'll also need Python (supported 3.7 and 3.8 versions) and pip installed.

### Building from source 🛠️

Just clone the ophelia repo and import Ophilea:
```sh   
git clone https://github.com/LuisFalva/ophilea.git
```

To initialize Ophilea with Spark embedded session we use:
```python
>>> from ophilea.start import Ophilea
>>> ophilea = Ophilea("Set Your Own Spark App Name")
>>> sc = ophilea.Spark.build_spark_context()

13:17:48.840 Ophilea [TAPE] +---------------------------------------------------------------+
13:17:48.840 Ophilea [INFO] | Hello! This API builds data mining & ml pipelines with pyspark|
13:17:48.840 Ophilea [INFO] | Welcome to Ophilea pyspark miner engine                       |
13:17:48.840 Ophilea [INFO] | API Version ophilea.0.0.1                                     |
13:17:48.840 Ophilea [TAPE] +---------------------------------------------------------------+
13:17:48.840 Ophilea [WARN]                      - Ophilea Gentleman Org -            
13:17:48.840 Ophilea [MASK]   █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ 
13:17:48.840 Ophilea [MASK]   █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ 
13:17:48.841 Ophilea [MASK]   █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ 
13:17:48.841 Ophilea [MASK]   █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.841 Ophilea [MASK]   █ ╬ ╬ ╬ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ ╬ ╬ ╬ █ 
13:17:48.841 Ophilea [MASK]   █ ╬ ╬ █ █ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ █ █ ╬ ╬ █ 
13:17:48.841 Ophilea [MASK]   █ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ █ 
13:17:48.841 Ophilea [MASK]   █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.841 Ophilea [MASK]   █ ╬ ╬ ╬ ╬ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ ╬ ╬ ╬ ╬ █ 
13:17:48.841 Ophilea [MASK]   █ ╬ ╬ █ █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █ █ ╬ ╬ █ 
13:17:48.841 Ophilea [MASK]   █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.841 Ophilea [MASK]   █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.841 Ophilea [MASK]   █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.841 Ophilea [MASK]   █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.841 Ophilea [MASK]   █ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ ╬ █ 
13:17:48.841 Ophilea [MASK]   █ ╬ ╬ ▓ ▓ ▓ ▓ ▓ ▓ ╬ ╬ █ ╬ ╬ ╬ █ ╬ ╬ ╬ █ ╬ ╬ ▓ ▓ ▓ ▓ ▓ ▓ ╬ ╬ █ 
13:17:48.841 Ophilea [MASK]   █ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ █ █ ╬ ╬ ╬ █ ╬ ╬ ╬ █ █ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ ╬ █ 
13:17:48.842 Ophilea [MASK]   █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.842 Ophilea [MASK]   █ ╬ ╬ ╬ ╬ ╬ █ █ █ █ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ █ █ █ █ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.842 Ophilea [MASK]   █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.842 Ophilea [MASK]   █ █ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ █ █ 
13:17:48.842 Ophilea [MASK]   █ █ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ █ █ 
13:17:48.842 Ophilea [MASK]   █ █ ╬ ╬ ▓ █ █ █ ╬ ╬ ╬ █ █ █ █ ╬ █ █ █ █ ╬ ╬ ╬ █ █ █ ▓ ╬ ╬ █ █ 
13:17:48.842 Ophilea [MASK]   █ █ █ ╬ ╬ ▓ ▓ █ █ █ █ █ █ █ ╬ ╬ ╬ █ █ █ █ █ █ █ ▓ ▓ ╬ ╬ █ █ █ 
13:17:48.842 Ophilea [MASK]   █ █ █ ╬ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ╬ ╬ ╬ ╬ █ █ █ 
13:17:48.842 Ophilea [MASK]   █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ 
13:17:48.842 Ophilea [MASK]   █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ 
13:17:48.842 Ophilea [MASK]   █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ 
13:17:48.842 Ophilea [MASK]   █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ 
13:17:48.842 Ophilea [MASK]   █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █ 
13:17:48.842 Ophilea [MASK]   █ █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █ █ 
13:17:48.842 Ophilea [MASK]   █ █ █ █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █ █ █ █ 
13:17:48.842 Ophilea [MASK]   █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ 
                                                              
13:17:48.843 Ophilea [WARN] Initializing Spark Session
13:17:58.062 Ophilea [INFO] Spark Version: 3.0.0
13:17:58.063 Ophilea [INFO] This Is: 'Set Your Own Spark App Name' App
13:17:58.063 Ophilea [INFO] Spark Context Initialized Success
```
Main class objects provided by initializing Ophilea session:

- Reader & Writer
```python
>>> from ophilea.read.spark_read import Read
>>> from ophilea.write.spark_write import Write
```
- Generic & Functions
```python
>>> from ophilea.functions import Shape, Rolling, Reshape, CorrMat, CrossTabular, PctChange, Selects, DynamicSampling
>>> from ophilea.generic import (split_date, row_index, lag_min_max_data, regex_expr, remove_duplicate_element,
                                 year_array, dates_index, sorted_date_list, feature_pick, binary_search, century_from_year,
                                 simple_average, delta_series, simple_moving_average, average, weight_moving_average,
                                 single_exp_smooth, double_exp_smooth, initial_seasonal_components, triple_exp_smooth,
                                 row_indexing, string_match)
```
- ML package for Unsupervised, Sampling and Feature Miner objects
```python
>>> from ophilea.ml.sampling.synthetic_sample import SyntheticSample
>>> from ophilea.ml.unsupervised.feature import PCAnalysis, SingularVD
>>> from ophilea.ml.feature_miner import BuildStringIndex, BuildOneHotEncoder, BuildVectorAssembler, BuildStandardScaler, SparkToNumpy, NumpyToVector
```
Let's show you some application examples:

The `Read` class implements Spark reading object in multiple formats `{'csv', 'parquet', 'excel', 'json'}`

```python
>>> from ophilea.read.spark_read import Read
>>> spark_df = spark.readFile(path, 'csv', header=True, infer_schema=True)
```

Also we can import class `Shape` from factory `functions` in order to see the dimension of our spark DataFrame such like numpy style.

```python
>>> from ophilea.functions import Shape
>>> dic = {
    'Product': ['A', 'B', 'C', 'A', 'B', 'C', 'A', 'B', 'C'],
    'Year': [2010, 2010, 2010, 2011, 2011, 2011, 2012, 2012, 2012],
    'Revenue': [100, 200, 300, 110, 190, 320, 120, 220, 350]
}
>>> dic_to_df = spark.createDataFrame(pd.DataFrame(data=dic))
>>> dic_to_df.show(10, False)

+-------+----+-------+
|Product|Year|Revenue|
+-------+----+-------+
|A      |2010|100    |
|B      |2010|200    |
|C      |2010|300    |
|A      |2011|110    |
|B      |2011|190    |
|C      |2011|320    |
|A      |2012|120    |
|B      |2012|220    |
|C      |2012|350    |
+-------+----+-------+

>>> dic_to_df.Shape
(9, 3)
```

The `pct_change` wrapper is added to the Spark `DataFrame` class in order to have the must commonly used method in Pandas
objects, this is for getting the relative percentage change between one observation to another sorted by some sortable 
date-type column and lagged by some laggable numeric-type column.

```python
>>> from ophilea.functions import PctChange
>>> dic_to_df.pctChange().show(10, False)

+-------------------+
|Revenue            |
+-------------------+
|null               |
|1.0                |
|0.5                |
|-0.6333333333333333|
|0.7272727272727273 |
|0.6842105263157894 |
|-0.625             |
|0.8333333333333333 |
|0.5909090909090908 |
+-------------------+
```

Another option is configuring all receiving parameters from the function, the following are:
- periods; this parameter will control the offset of the lag periods since the default value is 1 this will always return a lag-1 information DataFrame
- partition_by; the partition parameter will fixed the partition column over the DataFrame e.g. 'bank_segment', 'assurance_product_type'
- order_by; order by parameter will be the specific column to order the sequential observations, e.g. 'balance_date', 'trade_close_date', 'contract_date'
- pct_cols; percentage change col (pct_cols) will be the specific column to lag-over giving back the relative change between one element to other, e.g. 𝑥𝑡 ÷ 𝑥𝑡 − 1

In this case we will specify only the periods parameter to yield a lag of -2 days over the DataFrame
```python
>>> dic_to_df.pctChange(periods=2).na.fill(0).show(5, False)

+--------------------+
|Revenue             |
+--------------------+
|0.0                 |
|0.0                 |
|2.0                 |
|-0.44999999999999996|
|-0.3666666666666667 |
+--------------------+
only showing top 5 rows
```

Adding parameters: 'partition_by', 'order_by' & 'pct_cols'
```python
>>> dic_to_df.pctChange(partition_by="Product", order_by="Year", pct_cols="Revenue").na.fill(0).show(5, False)

+---------------------+
|Revenue              |
+---------------------+
|0.0                  |
|-0.050000000000000044|
|0.1578947368421053   |
|0.0                  |
|0.06666666666666665  |
+---------------------+
only showing top 5 rows
```

You also can lag more than one column at the same time, you just need to add a list with string column names:
```python
>>> dic_to_df.pctChange(partition_by="Product", order_by="Year", pct_cols=["Year", "Revenue"]).na.fill(0).show(5, False)

+--------------------+---------------------+
|Year                |Revenue              |
+--------------------+---------------------+
|0.0                 |0.0                  |
|4.975124378110429E-4|-0.050000000000000044|
|4.972650422674363E-4|0.1578947368421053   |
|0.0                 |0.0                  |
|4.975124378110429E-4|0.06666666666666665  |
+--------------------+---------------------+
only showing top 5 rows
```
 
### Planning to contribute? 🤔

Bring it on! If you have any idea or want to ask something or there is a bug you may want to fix you can open an [issue ticket](https://github.com/LuisFalva/ophilea/issues), there you will find all the alignments to make an issue request. Also here you can get a glimpse on [Open Source Contribution Guide best practicies](https://opensource.guide/).
Cheers 🍻!

### Support or Contact 📠

Having trouble with Ophilea? Yo can DM me to falvaluis@gmail.com and I’ll help you sort it out.

### License 📃

Released under the Apache License, version 2.0.
