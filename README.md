# Welcome to [Ophelia Spark](https://ophelia.readme.io/)

## 📝 Generalized ML Features

Our project focuses on creating robust and efficient PySpark ML and Mllib pipelines, making them easily replicable and secure for various machine learning tasks. Key features include optimized techniques for handling data skewness, user-friendly interfaces for building custom models, and streamlined data mining pipelines with Ophelia spark wrappers. Additionally, it functions as an emulator of NumPy and pandas, offering similar functionalities for a seamless user experience. Below are the detailed features:

- **Building PySpark ML & Mllib Pipelines**: Simplified and secure methods to construct machine learning pipelines using PySpark, ensuring replicability and robustness.
- **Optimized Techniques for Data Skewness**: Embedded strategies to address and mitigate data skewness issues, improving model performance and accuracy.
- **Build-Your-Own Models**: User-friendly tools for constructing custom models and data mining pipelines, leveraging the power of PySpark and Ophelia spark wrappers for enhanced flexibility and efficiency.
- **NumPy and pandas Functionality Emulation**: Emulates the functions and features of NumPy and pandas, making it intuitive and easy for users familiar with these libraries to transition and utilize similar functionalities within PySpark.

These features aim to empower users with the tools they need to handle complex machine learning tasks effectively, ensuring a seamless experience from data processing to model deployment.

# Getting Started:

### Requirements 📜

Before starting, you'll need to have installed pyspark >= 3.0.x, pandas >= 1.1.3, numpy >= 1.19.1, dask >= 2.30.x, scikit-learn >= 0.23.x 

Additionally, if you want to use the Ophelia package, you'll also need Python (supported 3.7 and 3.8 versions) and pip installed.

### Building from the source 🛠️

Just clone the `Ophelia` repo and import `Ophelia`:
```sh   
git clone https://github.com/LuisFalva/ophelia.git
```

After wiring and clone the `Ophelia` repo go to:
```sh   
cd ophelia_spark
```
### First time installation 📡
> For the very first time running and installing Ophelia in your local machine you need to wire with Ophelia's main repo.
Just run the following script in order to set up correctly:

And execute the following `make` instruction:
```sh   
make install
```

**First Important Note**: You must see a successful message like the one below.
```sh   
[Ophelia] Successfully installed ophelia_spark:0.1.0. Have fun! =)
```

**Second Important Note**: You also can pull Ophelia 0.1.0
[or make sure version matches with the one you need and configure the env `OPHELIA_DOCKER_VERSION`]
docker image and use it as base image for new images.
```sh   
make docker-pull
```

Also, you can push new changes to your corresponding version as follows:
```sh   
make docker-build
```

### Importing and initializing Ophelia 📦

To initialize `Ophelia` with Spark embedded session use:

```python
>>> from ophelia_spark.start import Ophelia
>>> ophelia = Ophelia("Spark App Name")
>>> sc = ophelia.Spark.build_spark_context()

  ____          _            _  _           _____                      _    
 / __ \        | |          | |(_)         / ____|                    | |   
| |  | | _ __  | |__    ___ | | _   __ _  | (___   _ __    __ _  _ __ | | __
| |  | || '_ \ | '_ \  / _ \| || | / _` |  \___ \ | '_ \  / _` || '__|| |/ /
| |__| || |_) || | | ||  __/| || || (_| |  ____) || |_) || (_| || |   |   < 
 \____/ | .__/ |_| |_| \___||_||_| \__,_| |_____/ | .__/  \__,_||_|   |_|\_\
        | |                                       | |                       
        |_|                                       |_|                       

```
Main class objects provided by initializing Ophelia session:

- `read` & `write`

```python
from ophelia_spark.read.spark_read import Read
from ophelia_spark.write.spark_write import Write
```
- `generic` & `functions`

```python
from ophelia_spark.functions import Shape, Rolling, Reshape, CorrMat, CrossTabular, PctChange, Selects, DynamicSampling
from ophelia_spark.generic import (split_date, row_index, lag_min_max_data, regex_expr, remove_duplicate_element,
                                   year_array, dates_index, sorted_date_list, feature_pick, binary_search,
                                   century_from_year, simple_average, delta_series, simple_moving_average, average,
                                   weight_moving_average, single_exp_smooth, double_exp_smooth, initial_seasonal_components,
                                   triple_exp_smooth, row_indexing, string_match)
```
- ML package for `unsupervised`, `sampling` and `feature_miner` objects

```python
from ophelia_spark.ml.sampling.synthetic_sample import SyntheticSample
from ophelia_spark.ml.unsupervised.feature import PCAnalysis, SingularVD
from ophelia_spark.ml.feature_miner import BuildStringIndex, BuildOneHotEncoder, BuildVectorAssembler, BuildStandardScaler, SparkToNumpy, NumpyToVector
```

Let me show you some application examples:

The `Read` class implements Spark reading object in multiple formats `{'csv', 'parquet', 'excel', 'json'}`

```python
from ophelia_spark.read.spark_read import Read
spark_df = spark.readFile(path, 'csv', header=True, infer_schema=True)
```

Also, you may import class `Shape` from factory `functions` in order to see the dimension of our spark DataFrame such as numpy style.

```python
from ophelia_spark.functions import Shape
dic = {
    'Product': ['A', 'B', 'C', 'A', 'B', 'C', 'A', 'B', 'C'],
    'Year': [2010, 2010, 2010, 2011, 2011, 2011, 2012, 2012, 2012],
    'Revenue': [100, 200, 300, 110, 190, 320, 120, 220, 350]
}
dic_to_df = spark.createDataFrame(pd.DataFrame(data=dic))
dic_to_df.show(10, False)

+---------+------------+-----------+
| Product |    Year    |  Revenue  |
+---------+------------+-----------+
|    A    |    2010    |    100    |
|    B    |    2010    |    200    |
|    C    |    2010    |    300    |
|    A    |    2011    |    110    |
|    B    |    2011    |    190    |
|    C    |    2011    |    320    |
|    A    |    2012    |    120    |
|    B    |    2012    |    220    |
|    C    |    2012    |    350    |
+---------+------------+-----------+

dic_to_df.Shape
(9, 3)
```

The `pct_change` wrapper is added to the Spark `DataFrame` class in order to have the most commonly used method in Pandas
objects to get the relative percentage change from one observation to another, sorted by a date-type column and lagged by a numeric-type column.

```python
from ophelia_spark.functions import PctChange
dic_to_df.pctChange().show(10, False)

+---------------------+
| Revenue             |
+---------------------+
| null                |
| 1.0                 |
| 0.5                 |
| -0.6333333333333333 |
| 0.7272727272727273  |
| 0.6842105263157894  |
| -0.625              |
| 0.8333333333333333  |
| 0.5909090909090908  |
+---------------------+
```

Another option is to configure all receiving parameters from the function, as follows:
- `periods`; this parameter will control the offset of the lag periods. Since the default value is 1, this will always return a lag-1 information DataFrame.
- `partition_by`; this parameter will fix the partition column over the DataFrame, e.g. 'bank_segment', 'assurance_product_type'.
- `order_by`; order by parameter will be the specific column to order the sequential observations, e.g. 'balance_date', 'trade_close_date', 'contract_date'.
- `pct_cols`; percentage change col (pct_cols) will be the specific column to lag-over giving back the relative change between one element to other, e.g. 𝑥𝑡 ÷ 𝑥𝑡 − 1

In this case, we will specify only the `periods` parameter to yield a lag of -2 days over the DataFrame.
```python
dic_to_df.pctChange(periods=2).na.fill(0).show(5, False)

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
dic_to_df.pctChange(partition_by="Product", order_by="Year", pct_cols="Revenue").na.fill(0).show(5, False)

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
dic_to_df.pctChange(partition_by="Product", order_by="Year", pct_cols=["Year", "Revenue"]).na.fill(0).show(5, False)

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

Having trouble with Ophilea? Yo can DM me at [falvaluis@gmail.com](https://mail.google.com/mail/u/0/?tab=rm&ogbl#inbox?compose=CllgCJZZQVJHBJKmdjtXgzlrRcRktFLwFQsvWKqcTRtvQTVcHvgTNSxVzjZqjvDFhZlVJlPKqtg) and I’ll help you sort it out.

### License 📃

Released under the Apache License, version 2.0.
