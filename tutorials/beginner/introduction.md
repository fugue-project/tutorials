---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.10.3
kernelspec:
  display_name: Python 3.7.9 64-bit
  name: python3
---

# Introduction

+++

When data is small enough to fit on a laptop, it's simple for data practitioners to iterate on data projects. Most commonly, data practitioners use pandas and NumPy for their data analysis and feature engineering needs. These tools go well with scikit-learn to provide a stack capable of handling the end-to-end machine learning pipeline. This works great until data becomes too big to fit on a single machine. At this point, data practitioners need to move to distributed compute frameworks such as Spark and Dask to scale their solutions out.

+++

## Utilizing Distributed Compute

pandas is great for small datasets, but unfortunately does not scale well large datasets. The primary reason is that pandas is single core, and does not take advantage of all available compute resources. A lot of operations also generate [intermediate copies](https://pandas.pydata.org/pandas-docs/stable/user_guide/scale.html#scaling-to-large-datasets) of data, utilizing more memory than necessary. To effectively handle data with pandas, users preferably need to have [5x to 10x times](https://wesmckinney.com/blog/apache-arrow-pandas-internals/) as much RAM as the size of the dataset.

This leads us to frameworks such as Spark and Dask. These frameworks allow us to split compute jobs across multiple machines. They also can handle datasets that don’t fit into memory by spilling data over to disk in some cases. Compared to Spark, Dask is the easier transition from pandas because it is built on top of the pandas DataFrames which means that there is strong parity between their APIs. But ultimately, moving to Spark or Dask still requires code changes to port pandas code. Added to changing code, there is also a big shift in mindset needed to fully utilize the distributed compute engines. 

**Fugue is a framework that is designed to unify the interface between pandas, Spark, and Dask, allowing one codebase to be used across all three engines.**

+++

## Fugue `transform`

Fugue is an abstraction layer designed to provide a seamless transition from local compute to distributed compute. Using the abstraction layer allows users to take advantage of the Spark and Dask computation engines, while writing code in Python, pandas or SQL. This allows users to focus on the problems they are trying to solve, rather than learning a new framework for the job. This also provides other concrete benefits that we’ll see throughout this tutorial.

The simplest way Fugue can be used to scale Pandas based code to Spark or Dask is the `transform` function. In the example below, we’ll train a model using scikit-learn and pandas, and then perform the `predict` step in parallel on top of the Spark execution engine.

```{code-cell} ipython3
import pandas as pd
import numpy as np
from sklearn.linear_model import LinearRegression

X = pd.DataFrame({"x_1": [1, 1, 2, 2], "x_2":[1, 2, 2, 3]})
y = np.dot(X, np.array([1, 2])) + 3
reg = LinearRegression().fit(X, y)
```

After training our model, we then wrap it in a `predict` function. This function is still written in pandas. We can easily test it on the `input_df` that we create.

```{code-cell} ipython3
def predict(df: pd.DataFrame, model: LinearRegression) -> pd.DataFrame:
    return df.assign(predicted=model.predict(df))

input_df = pd.DataFrame({"x_1": [3, 4, 6, 6], "x_2":[3, 3, 6, 6]})

# test the function
predict(input_df.copy(), reg)
```

Now we bring it to Spark using Fugue in the next code snippets. Fugue has a function called transform that takes in a DataFrame and applies a function to it distributedly using Spark engine or Dask engine. We’ll explain the inputs that go into this function in a bit (but they should be intuitive). The important thing to notice is that we did not make modifications to the pandas-based `predict` function in order to use it on Spark. This function can now scale to big datasets through the Spark execution engine.

Even if there is no cluster available, the `SparkExecutionEngine` will start a local Spark instance and parallelize the jobs with all cores of the machine

```{code-cell} ipython3
:tags: [remove-stderr]
# create Spark session for next cells
from pyspark.sql import SparkSession
spark_session = SparkSession.builder.getOrCreate()
```

```{code-cell} ipython3
from fugue import transform
from fugue_spark import SparkExecutionEngine

result = transform(
    input_df,
    predict,
    schema="*,predicted:double",
    params=dict(model=reg),
    engine=SparkExecutionEngine(spark_session)
)
result.show()
```

The first two arguments of the `transform` function are the DataFrame to operate on and the function to use. The `input_df` can either be a pandas DataFrame or a Spark DataFrame. The engine then dictates what execution engine to use for the computation. Because we supplied a pandas DataFrame with the `SparkExecutionEngine`, that DataFrame was converted to be used in Spark. The output of this function is a Spark DataFrame because the engine used was the `SparkExecutionEngine`. Supplying no engine uses the pandas-based `NativeExecutionEngine`. Fugue also has a `DaskExecutionEngine` available.

The other two arguments are the `schema` and `params`. Explicit `schema` is a hard requirement in distributed computing frameworks, so we need to supply the output `schema` of the operation. When compared to the Spark equivalent (seen below), this is a much simpler interface to handle the `schema`. Lastly, `params` is a dictionary that contains other inputs into the function. In this case, we passed in the regression model to be used.

+++

## Conclusion

With that, we have shown the use-case of Fugue in scaling pandas-written code to Spark. It can be done in very few lines of code, without altering the existing code base. In the next section, we’ll see other features Fugue has to offer, and the other ways it simplifies using distributed compute. By using the `transform` function, we allowed the `predict` function to be applicable to both pandas and Spark. We’ll apply this same concept to entire workflows in the next section.

While we used pandas here, we’ll also show that native Python functions can also be used across the different execution engines.

+++

## [Optional] Spark Equivalent of `transform`

If you are wondering how `transform` compares to implementing the same logic in Spark, below is an example of how the pandas function would be implemented in Spark if you did it yourself. This implementation uses the Spark’s `mapInPandas` method available in Spark 3.0. Note how the `schema` has to be handled inside the `run_predict` function. This is the `schema` requirement we mentioned earlier that Fugue provides a simpler interface for.

```{code-cell} ipython3
from typing import Iterator, Any, Union
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql import DataFrame, SparkSession

def predict_wrapper(dfs: Iterator[pd.DataFrame], model):
    for df in dfs:
        yield predict(df, model)

def run_predict(input_df: Union[DataFrame, pd.DataFrame], model):
    # conversion
    if isinstance(input_df, pd.DataFrame):
        sdf = spark_session.createDataFrame(input_df.copy())
    else:
        sdf = input_df.copy()

    schema = StructType(list(sdf.schema.fields))
    schema.add(StructField("predicted", DoubleType()))
    return sdf.mapInPandas(lambda dfs: predict_wrapper(dfs, model), 
                           schema=schema)

result = run_predict(input_df.copy(), reg)
result.show()
```

It’s very easy to see why it becomes very difficult to bring a pandas codebase to Spark with this approach. We had to define two additional functions in the `predict_wrapper` and the `run_predict` to bring it to Spark. If this had to be done for tens of functions, it could easily fill the codebase with boilerplate code, making it hard to focus on the logic.
