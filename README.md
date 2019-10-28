# psychic-octo-tribble

#### In order to execute correctly all workflow, please follow this execution order:

##### 1-. Transfrom Data notebook, this module will read resources/.csv file and transform them to parquet.
##### 2-. If need so, you can execute the Exploratory analysis notebook reading parquet persist data.
##### 3-. Run Single Value Decomposition, this must return with Principle Components arrays (Dev).
##### 4-. The Optimization example notebook is just for visualizing in R2 a traditional optimization problem.
##### 5-. This is the most important notebook, this one will take al matrix decomposition for RiskParity optimization (Dev).
##### 6-. Sharpe Ratio notebook is the first approach in a optimizing fund selection.

##### Note: All notebook will create path files on your root local file directory. 
