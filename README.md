# [Ophel.ia](https://luisfalva.github.io/ophelia/)

*Ophelia* ("Hamlet's beloved beautiful woman") she's known because of her madness and immortal love for Hamlet, but the full Shakespeare's mastery piece does not favor to her magnificent character.
Ophelia is the epitome of goodness, the brightness and the elegance of simplicity.

# Motivations 🚀

As Data Scientist or Data Analyst don't want to waist too much time guessing how damn 😬 we may use PySpark's API in a correct and optimized way, sometimes we just want a prompt answer instead of a full nicely code. With the intent of that, this project
aims to help to reduce the complexity of the analytical lifecycle for every DA or DS who uses PySpark frequently.

Now is the turn of a new smart and very extravaganza Ophelia serving to optimize the learning curve with PySpark's must common functionality features such as:
- Build PySpark *ML & Mllib* pipelines in a simplified replicable and secure way
- Embedded optimized techniques helping users struggling with *data skewness problems*
- Easy to use and build your own models and data mining pipelines with PySpark using *Ophelia spark wrappers*
- Security and simplified usage for exploring new *PySpark features* for data mining replicating the most commonly used functionality in libraries such as Pandas and Numpy
- Simple *Pythonic* syntax: *'Not too fancy things to do the hard work'*
- Utility for *RDD level pre-processing* data in a simple manner
- Time series treatment and portfolio optimization with different techniques based on Portfolio Theory such as Risk Parity, Efficient Frontier, Clustering by Sortion's ratio and Sharpe's ratio, among others

It is time for Ophelia's vendetta and claim for her brightness...

# Getting Started:


### Requirements 📜

Before starting, you'll need to have installed pyspark >= 3.0.x, pandas >= 1.1.3, numpy >= 1.19.1, dask >= 2.30.x, scikit-learn >= 0.23.x 
Additionally, if you want to use the Ophelia API, you'll also need Python (supported 3.7 and 3.8 versions) and pip installed.

### Building from source 🛠️

Just clone the ophelia repo and import Ophelia:

```sh   
git clone https://github.com/LuisFalva/ophelia.git
```

To initialize Ophelia with Spark embedded session we use:

```python
>>> from ophelia.ophelia_start import Ophelia
>>> ophelia = Ophelia("Set Your Own Spark App Name")
>>> sc = ophelia.Spark.build_spark_context()

13:17:48.840 Ophelia [TAPE] +----------------------------------------------------------------+
13:17:48.840 Ophelia [INFO] | Hello! This API builds data mining & ml pipelines with pyspark |
13:17:48.840 Ophelia [INFO] | Welcome to Ophelia pyspark miner engine                        |
13:17:48.840 Ophelia [INFO] | API Version Ophelia.0.0.1                                      |
13:17:48.840 Ophelia [WARN] | V for Vendata...                                               |
13:17:48.840 Ophelia [TAPE] +----------------------------------------------------------------+
13:17:48.840 Ophelia [TAPE] +----------------------------------------------------------------+
13:17:48.840 Ophelia [WARN]                 - Ophelia's Fellowship Gentleman -            
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
13:17:48.842 Ophelia [WARN]                                                               
13:17:48.843 Ophelia [TAPE] +----------------------------------------------------------------+
13:17:48.843 Ophelia [WARN] Initializing Spark Session
13:17:58.062 Ophelia [INFO] Spark Version: 3.0.0
13:17:58.063 Ophelia [INFO] This Is: 'Set Your Own Spark App Name' App
13:17:58.063 Ophelia [INFO] Spark Context Initialized Success
```

Main class functions:

```python
>>> from ophelia.read.spark_read import Read
>>> from ophelia.ml.feature_miner import FeatureMiner
>>> from ophelia.ml.unsupervised.featured import SingularVD
>>> from ophelia.ml.sampling.synthetic_sample import SyntheticSample
>>> from ophelia.functions import Shape, Rolling, Reshape, CorrMat, CrossTabular, PctChange, Selects, DynamicSampling
>>> from ophelia.func_utils import DataFrameUtils, ListUtils, RDDUtils
```

Lets show you some application examples:

The `Read` class implements Spark reading object in multiple formats `{'csv', 'parquet', 'excel', 'json'}`

```python
>>> spark_df = spark.readFile(path, 'csv', header=True, infer_schema=True)
```

Also we can import class `Shape` from factory `functions` in order to see the dimension of our spark DataFrame such like numpy style.

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

The `pct_change` wrapper is added to the Spark `DataFrame` class in order to have the must commonly used method in Pandas
objects, this is for getting the relative percentage change between one observation to another sorted by some sortable 
date-type column and lagged by some laggable numeric-type column.

```python
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
 
### Planning to contribute? 🤔

Bring it on! If you have any idea or want to ask something or there is a bug you may want to fix you can open an [issue ticket](https://github.com/LuisFalva/ophelia/issues), there you will find all the alignments to make an issue request. Also here you can get a glimpse on [Open Source Contribution Guide best practicies](https://opensource.guide/).
Cheers 🍻!

### Support or Contact 📠

Having trouble with Ophelia? Yo can DM me to falvaluis@gmail.com and I’ll help you sort it out.

### License 📃

Released under the Apache License, version 2.0.
