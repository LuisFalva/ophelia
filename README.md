# [Ophilea](https://luisfalva.github.io/ophilea/)

*Ophelia* Hamlet's beloved beautiful woman (and is the name of the package too), is known because of her madness and immortal love for Hamlet; but Shakespeare's entire master piece does not do justice to her magnificent character.
Ophelia is the epitome of goodness, brightness, and the elegance of simplicity.

# Motivations 🚀

As Data Scientists or Data Analysts, we don't really want to waste too much time guessing how PySpark's framework may be used. Sometimes we just want a prompt answer instead of a full nice code. With that in mind, this project
aims to help reduce the complexity of the analytical lifecycle for everyone who uses PySpark frequently.

Now is the time of a new, smart, and very extravagant `Ophelia` to help us optimize the learning curve involved in PySpark's most common functionality, offering features such as:
- Building PySpark *ML & Mllib* pipelines in a simplified replicable and secure way
- Embedded optimized techniques to help users struggling with *data skewness problems*
- Easy to use build-your-own models and data mining pipelines with PySpark using *Ophilea spark wrappers*
- Security and simplified usage for exploring new *PySpark features* for data mining replicating the most commonly used functionality in libraries such as Pandas and Numpy
- Simple *Pythonic* syntax: *'Not too fancy things to do the hard work'*
- Utility for *RDD level pre-processing* data in a simple manner
- Time series treatment and portfolio optimization with different techniques based on Portfolio Theory such as Risk Parity, Efficient Frontier, Clustering by Sortion's ratio and Sharpe's ratio, among others

# Getting Started:

### Requirements 📜

Before starting, you'll need to have installed pyspark >= 3.0.x, pandas >= 1.1.3, numpy >= 1.19.1, dask >= 2.30.x, scikit-learn >= 0.23.x 
Additionally, if you want to use the Ophelia package, you'll also need Python (supported 3.7 and 3.8 versions) and pip installed.

### Building from source 🛠️

Just clone the `Ophelia` repo and import `Ophelia`:
```sh   
git clone https://github.com/LuisFalva/ophilea.git
```

After clone the `Ophelia` repo go to:
```sh   
cd ophilea
```

And execute the following bash script:
```sh   
cd ./build/ophelia_install.sh
```

**Important Note**: You must see a successful message like this one below:
```sh   
Successfully installed Ophelia-0.1.dev0
********************************************************
Ophelia is now installed in your local env. Have fun! =)
********************************************************
```
### Importing and initializing Ophelia 📦

To initialize `Ophelia` with Spark embedded session use:
```python
>>> from ophelia.start import Ophelia
>>> ophelia = Ophelia("Set Your Own Spark App Name")
>>> sc = ophelia.Spark.build_spark_context()

13:17:48.840 Ophelia [TAPE] +---------------------------------------------------------------+
13:17:48.840 Ophelia [INFO] | Hello! This API builds data mining & ml pipelines with pyspark|
13:17:48.840 Ophelia [INFO] | Welcome to Ophelia pyspark miner engine                       |
13:17:48.840 Ophelia [INFO] | Lib Version ophelia.0.1.dev0                                  |
13:17:48.840 Ophelia [TAPE] +---------------------------------------------------------------+
13:17:48.840 Ophelia [WARN]                      - Ophilea Gentleman Org -            
13:17:48.840 Ophelia [MASK]   █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ 
13:17:48.840 Ophelia [MASK]   █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ 
13:17:48.841 Ophelia [MASK]   █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ 
13:17:48.841 Ophelia [MASK]   █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.841 Ophelia [MASK]   █ ╬ ╬ ╬ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ ╬ ╬ ╬ █ 
13:17:48.841 Ophelia [MASK]   █ ╬ ╬ █ █ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ █ █ ╬ ╬ █ 
13:17:48.841 Ophelia [MASK]   █ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ █ 
13:17:48.841 Ophelia [MASK]   █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.841 Ophelia [MASK]   █ ╬ ╬ ╬ ╬ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ ╬ ╬ ╬ ╬ █ 
13:17:48.841 Ophelia [MASK]   █ ╬ ╬ █ █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █ █ ╬ ╬ █ 
13:17:48.841 Ophelia [MASK]   █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.841 Ophelia [MASK]   █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.841 Ophelia [MASK]   █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.841 Ophelia [MASK]   █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.841 Ophelia [MASK]   █ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ ╬ █ 
13:17:48.841 Ophelia [MASK]   █ ╬ ╬ ▓ ▓ ▓ ▓ ▓ ▓ ╬ ╬ █ ╬ ╬ ╬ █ ╬ ╬ ╬ █ ╬ ╬ ▓ ▓ ▓ ▓ ▓ ▓ ╬ ╬ █ 
13:17:48.841 Ophelia [MASK]   █ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ █ █ ╬ ╬ ╬ █ ╬ ╬ ╬ █ █ ╬ ╬ ▓ ▓ ▓ ▓ ╬ ╬ ╬ █ 
13:17:48.842 Ophelia [MASK]   █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.842 Ophelia [MASK]   █ ╬ ╬ ╬ ╬ ╬ █ █ █ █ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ █ █ █ █ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.842 Ophelia [MASK]   █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ 
13:17:48.842 Ophelia [MASK]   █ █ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ █ █ 
13:17:48.842 Ophelia [MASK]   █ █ ╬ ╬ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ ╬ ╬ █ █ 
13:17:48.842 Ophelia [MASK]   █ █ ╬ ╬ ▓ █ █ █ ╬ ╬ ╬ █ █ █ █ ╬ █ █ █ █ ╬ ╬ ╬ █ █ █ ▓ ╬ ╬ █ █ 
13:17:48.842 Ophelia [MASK]   █ █ █ ╬ ╬ ▓ ▓ █ █ █ █ █ █ █ ╬ ╬ ╬ █ █ █ █ █ █ █ ▓ ▓ ╬ ╬ █ █ █ 
13:17:48.842 Ophelia [MASK]   █ █ █ ╬ ╬ ╬ ╬ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ▓ ╬ ╬ ╬ ╬ █ █ █ 
13:17:48.842 Ophelia [MASK]   █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ 
13:17:48.842 Ophelia [MASK]   █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ 
13:17:48.842 Ophelia [MASK]   █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ 
13:17:48.842 Ophelia [MASK]   █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ 
13:17:48.842 Ophelia [MASK]   █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █ 
13:17:48.842 Ophelia [MASK]   █ █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ ╬ █ █ █ ╬ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █ █ 
13:17:48.842 Ophelia [MASK]   █ █ █ █ █ █ █ █ █ █ █ ╬ ╬ ╬ ╬ █ ╬ ╬ ╬ ╬ █ █ █ █ █ █ █ █ █ █ █ 
13:17:48.842 Ophelia [MASK]   █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ █ 
                                                              
13:17:48.843 Ophelia [WARN] Initializing Spark Session
13:17:58.062 Ophelia [INFO] Spark Version: 3.0.0
13:17:58.063 Ophelia [INFO] This Is: 'Set Your Own Spark App Name' App
13:17:58.063 Ophelia [INFO] Spark Context Initialized Success
```
Main class objects provided by initializing Ophelia session:

- `read` & `write`
```python
>>> from ophelia.read.spark_read import Read
>>> from ophelia.write.spark_write import Write
```
- `generic` & `functions`
```python
>>> from ophelia.functions import Shape, Rolling, Reshape, CorrMat, CrossTabular, PctChange, Selects, DynamicSampling
>>> from ophelia.generic import (split_date, row_index, lag_min_max_data, regex_expr, remove_duplicate_element,
                                 year_array, dates_index, sorted_date_list, feature_pick, binary_search, century_from_year,
                                 simple_average, delta_series, simple_moving_average, average, weight_moving_average,
                                 single_exp_smooth, double_exp_smooth, initial_seasonal_components, triple_exp_smooth,
                                 row_indexing, string_match)
```
- ML package for `unsupervised`, `sampling` and `feature_miner` objects
```python
>>> from ophelia.ml.sampling.synthetic_sample import SyntheticSample
>>> from ophelia.ml.unsupervised.feature import PCAnalysis, SingularVD
>>> from ophelia.ml.feature_miner import BuildStringIndex, BuildOneHotEncoder, BuildVectorAssembler, BuildStandardScaler, SparkToNumpy, NumpyToVector
```

Let me show you some application examples:

The `Read` class implements Spark reading object in multiple formats `{'csv', 'parquet', 'excel', 'json'}`

```python
>>> from ophelia.read.spark_read import Read
>>> spark_df = spark.readFile(path, 'csv', header=True, infer_schema=True)
```

Also, you may import class `Shape` from factory `functions` in order to see the dimension of our spark DataFrame such as numpy style.

```python
>>> from ophelia.functions import Shape
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

The `pct_change` wrapper is added to the Spark `DataFrame` class in order to have the most commonly used method in Pandas
objects to get the relative percentage change from one observation to another, sorted by a date-type column and lagged by a numeric-type column.

```python
>>> from ophelia.functions import PctChange
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

Another option is to configure all receiving parameters from the function, as follows:
- `periods`; this parameter will control the offset of the lag periods. Since the default value is 1, this will always return a lag-1 information DataFrame.
- `partition_by`; this parameter will fix the partition column over the DataFrame, e.g. 'bank_segment', 'assurance_product_type'.
- `order_by`; order by parameter will be the specific column to order the sequential observations, e.g. 'balance_date', 'trade_close_date', 'contract_date'.
- `pct_cols`; percentage change col (pct_cols) will be the specific column to lag-over giving back the relative change between one element to other, e.g. 𝑥𝑡 ÷ 𝑥𝑡 − 1

In this case, we will specify only the `periods` parameter to yield a lag of -2 days over the DataFrame.
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

Adding parameters: `partition_by`, `order_by` & `pct_cols`
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

You may also lag more than one column at a time by simply adding a list with string column names:
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
 
### Want to contribute? 🤔

Bring it on! If you have an idea or want to ask anything, or there is a bug you want fixed, you may open an [issue ticket](https://github.com/LuisFalva/ophilea/issues). You will find the guidelines to make an issue request there. Also, you can get a glimpse of [Open Source Contribution Guide best practices here](https://opensource.guide/).
Cheers 🍻!

### Support or Contact 📠

Having trouble with Ophilea? Yo can DM me at falvaluis@gmail.com and I’ll help you sort it out.

### License 📃

Released under the Apache License, version 2.0.
