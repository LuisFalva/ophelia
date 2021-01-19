# [Ophel.ia](https://luisfalva.github.io/ophelia/)

*Ophelia* ("Hamlet's beloved beautiful woman") she's known because of her madness and immortal love for Hamlet, but the full Shakespeare's mastery piece does not favor to her magnificent character.
Ophelia is the epitome of goodness, the brightness and the elegance of simplicity.

*I will not go so far as to affirm that to construct a history of human thought without a deep study of mathematical ideas in successive ages is like omitting Hamlet from the drama that bears his name.
That would be pretending too much. But it certainly amounts to excluding the character of Ophelia. This simile is particularly accurate. Ophelia is essential in the drama, she is very charming, and a little crazy.
Let us then admit that the study of mathematics is a divine folly of the human spirit, a refuge from the atrocious urgency of contingent events.*

*- Alfred North Whitehead -*

<p align="center">
  <img src="/docs/img/ophelia-mask.png" width="450" title="ophelia mask">
</p>

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
from ophelia.spark.start import Ophelia

ophelia = Ophelia("Set Your Own Spark App Name")
sc = ophelia.Spark.build_spark_context()

>>> 02:22:03.992 Ophelia [TAPE] +---------------------------------------------------------------------+
>>> 02:22:03.992 Ophelia [INFO] | My name is Ophelia Vendata                                          |
>>> 02:22:03.992 Ophelia [INFO] | I am an artificial assistant for data mining & ML engine with spark |
>>> 02:22:03.992 Ophelia [INFO] | Welcome to Ophelia spark miner engine                               |
>>> 02:22:03.992 Ophelia [INFO] | Lib Version Ophelia.0.0.1                                           |
>>> 02:22:03.992 Ophelia [WARN] | V for Vendata...                                                    |
>>> 02:22:03.992 Ophelia [TAPE] +---------------------------------------------------------------------+
>>> 01:56:47.933 Ophelia [WARN] Initializing Spark Session
>>> 01:56:56.867 Ophelia [INFO] Spark Version: 3.0.0
>>> 01:56:56.867 Ophelia [INFO] This Is: 'Set Your Own Spark App Name' App
>>> 01:56:56.867 Ophelia [INFO] Spark Context Initialized Success
```
    
Main class functions:

```python
from ophelia.spark.read.spark_read import Read
from ophelia.spark.ml.feature_miner import FeatureMiner
from ophelia.spark.ml.unsupervised.featured import SingularVD
from ophelia.spark.ml.sampling.synthetic_sample import SyntheticSample
from ophelia.spark.functions import Shape, Rolling, Reshape, CorrMat, CrossTabular, PctChange, Selects, DynamicSampling
from ophelia.spark.func_utils import DataFrameUtils, ListUtils, RDDUtils
```

Lets show you some application examples:

The `Read` class implements Spark reading object in multiple formats `{'csv', 'parquet', 'excel', 'json'}`

```python
spark_df = spark.readFile(path, 'csv', header=True, infer_schema=True)
```
    
### Planning to contribute? 🤔

Bring it on! If you have any idea or want to ask something or there is a bug you may want to fix you can open an [issue ticket](https://github.com/LuisFalva/ophelia/issues), there you will find all the alignments to make an issue request. Also here you can get a glimpse on [Open Source Contribution Guide best practicies](https://opensource.guide/).
Cheers 🍻!

### Support or Contact 📠

Having trouble with Ophelia? Yo can DM me to falvaluis@gmail.com and I’ll help you sort it out.

### License 📃

Released under the Apache License, version 2.0.