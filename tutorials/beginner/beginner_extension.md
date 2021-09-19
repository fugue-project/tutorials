---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.10.3
kernelspec:
  display_name: 'Python 3.7.9 64-bit (''fugue-tutorials'': conda)'
  metadata:
    interpreter:
      hash: 131b24c7e1bb8763ab2b04d5b6d98a68c7b3a823a2a57c5722935f7690890f70
  name: python3
---

# Extensions

The FugueWorkflow object creates a Directed Acyclic Graph (DAG) where the nodes are DataFrames that are connected by extensions. Extensions are code that creates/modifies/outputs DataFrames. The `transformer` we have been using is an example of an extension. In this section, we'll cover the other types of extensions: `creator`, `processor`, `outputter`, and `cotransformer`. For all extensions, schema has to be defined. Below are the types of extensions.

``` {image} images/extensions.svg
---
width: 700
name: Extensions
---
```

`outputtransformer` and `outputcotransformer` will be covered in the Deep Dive section. 

We have actually already seen some built-in extensions that come with Fugue. For example, `load` is a `creator` and `save` is an `outputter`. There is a difference between `Driver side` and `Worker side` extensions. This will be covered at the end of this section. For now, we'll just see the syntax and use case for each extension.

## Creator

A creator is an extension that takes no DataFrame as input, but returns a DataFrame as output. It is used to generate DataFrames. Custom creators can be used to load data from different sources (think AWS S3 or from a Database using pyodbc). Similar to the `transformer` in the previous section, `creators` can be defined with the schema hint comment, or with the `@creator` decorator. `pd.DataFrame` is a special output type that does not require schema. For other output type hints, the schema is unknown so it needs to be defined.


```{code-cell} ipython3
import pandas as pd
from fugue import FugueWorkflow
from typing import List, Dict, Any

def create_data() -> pd.DataFrame:
    df = pd.DataFrame({'a': [1,2,3], 'b': [2,3,4]})
    return df

# schema: a:int, b:int
def create_data2() -> List[Dict[str, Any]]:
    df = [{'a':1, 'b':2}, {'a':2, 'b':3}]
    return df

with FugueWorkflow() as dag:
     df = dag.create(create_data)
     df2 = dag.create(create_data2)
     df2.show()
```

## Processor

A `processor` is an extension that takes in one or more DataFrames, and then outputs one DataFrame. Similar to the `creator`, schema does not need to be specified for pd.DataFrame because it is already known. Schema needs to be specified for other output types. The processor can be defined using the schema hint, or the `@processor` decorator with the schema passed in.

```{code-cell} ipython3
from typing import List, Dict, Any

def concat(df1:pd.DataFrame, df2:pd.DataFrame) -> pd.DataFrame:
    return pd.concat([df1,df2]).reset_index(drop=True)

# schema: a:double, b:double
def fillna(df:List[Dict[str,Any]], n=0) -> List[Dict[str,Any]]:
    for row in df:
        if row['a'] is None:
            row['a'] = n
    return df

with FugueWorkflow() as dag:
     df = dag.create(create_data2)    # create_data2 is from earlier
     df2 = dag.create(create_data2)
     df3 = dag.process(df , df2, using=concat)
     df3 = dag.process(df3, using=fillna, params={'n': 10})
     df3.show()
```

Here we show an example for of a `fillna` processor, but this is a common operation so there is actually a built-in operation for it.

```{code-cell} ipython3
with FugueWorkflow() as dag:
     df = dag.create(create_data2)
     df = df.fillna(10, subset=["a"])
     df.show()
```

## Outputter

Outputters are extensions with one of more DataFrames as an input, and no DataFrames at the output. We mentioned earlier that `save` was an example of an outputter. `show` is actually another example. Outputters can be used to write to S3, or upload to database. The output type of Outputters must be `None`. No schema is needed since it is a terminal operation. There is an `@outputter` decorator, but it doesn't do much because the return type is already `None`. Outputters are also used for plotting functions.

```{code-cell} ipython3
def head(df:List[List[Any]], n=1) -> None:
    for i in range(n):
        print(df[i])

with FugueWorkflow() as dag:
    df = dag.create(create_data2)
    dag.output(df, using=head, params={'n': 2})
```

## Transformer

Transformer is the most widely used extension. We have covered this in previous sections but more formally, a transformer is an extension that takes in a DataFrame and returns a DataFrame. Schema needs to be explicit. Most logic will go into transformers. Below is an example to create a new column.

```{code-cell} ipython3
#schema: *, c:int
def sum_cols(df: pd.DataFrame) -> pd.DataFrame:
    df['c'] = df['a'] + df['b']
    return df

with FugueWorkflow() as dag:
    df = dag.create(create_data2).fillna(10)
    df = df.transform(using=sum_cols)
    df.show()
```

## CoTransformer

The `cotransformer` is very similar to the `transformer`, except that it is intended to execute on multiple DataFrames that are partitioned in the same way. The next section will cover CoTransformer, as it is coupled with distributed computing concepts that need to be introduced.

+++

## Summary

In this section we have gone over the building blocks of a FugueWorkflow in Fugue extensions. Extensions are abstractions for the different kinds of operations done on DataFrames. Fugue has the most common extensions built-in, but it will be very common for users to make their own extensions (especially transformers) to work with DataFrames.

+++

## [Optional] Driver Extensions vs Worker Extensions

For those unfamiliar with distributed systems, the work is spread across multiple workers, often referred to as a cluster. The driver is the machine that orchestrates the work done by the workers. For Fugue extensions, `transformer` and `cotransformer` are extensions that happen on the worker. Actions that happen on the worker-level are already agnostic to the ExecutionEngine.

On the other hand, driver side extensions are ExecutionEngine aware. This means that these extensions can use code written with Spark or Dask specifically. All we need to do is to pass a first argument with the ExecutionEngine type annotation.

```{code-cell} ipython3
:tags: [remove-stderr]
from fugue import ExecutionEngine, DataFrame
from fugue_spark import SparkExecutionEngine, SparkDataFrame

# pay attention to the input and output annotations, they are both general DataFrame
def create(e:ExecutionEngine, n=1) -> DataFrame:
    assert isinstance(e,SparkExecutionEngine) # this extension only works with SparkExecutionEngine
    sdf= e.spark_session.createDataFrame([[n]], schema="a:int")  # this is how you get spark session
    return SparkDataFrame(sdf) # you must wrap as Fugue SparkDataFrame to return

with FugueWorkflow(SparkExecutionEngine) as dag:
    dag.create(create, params={"n":2}).show()
```

In the code above, we lose cross-platform execution, but this can be used when users need to write Spark specific code. `createDataFrame` is a Spark specific method. This approach is Fugue's way of exposing the underlying ExecutionEngine if users want to use it. `creator`, `processor` and `outputter` are ExecutionEngine aware. For users who are not as familair with Spark, the recommendation is to write ExecutionEngine-agnostic code. That gives the most benefit of using Fugue because of the portability it provides.
