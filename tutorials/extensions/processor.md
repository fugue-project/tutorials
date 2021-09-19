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

# Processor

`Processor` represents the logic unit executing on driver on the **entire** input dataframes. While there is overlap with `Transformer`, transformers are more focused on the logic execution on a partition-level. 

In this tutorial are the methods to define a `Processor`. There is no preferred method and Fugue makes it flexible for users to choose whatever interface works for them. The four ways are native approach, schema hint, decorator, and the class interface in order of simplicity.

## Example Use Cases

* **Combining multiple DataFrames** into one like `concat`
* **Column-wise aggregates on the whole DataFrame**. For example, getting the standard deviation of a column.
* **Performing logic that requires Spark of Dask functions**

+++

## Quick Notes on Usage

**ExecutionEngine aware**

* Processors run on the driver so they are aware of the `ExecutionEngine` being used. Passing a parameter with the `ExecutionEngine` annotation will pass in the current `ExecutionEngine`. There is an example of this later.

**Acceptable input DataFrame types**

* `DataFrame`, `LocalDataFrame`, `pd.DataFrame`, `List[List[Any]]`, `Iterable[List[Any]]`, `EmptyAwareIterable[List[Any]]`, `List[Dict[str, Any]]`, `Iterable[Dict[str, Any]]`, `EmptyAwareIterable[Dict[str, Any]]`
* Input can also be Fugue `DataFrames`, which is a collection of Fugue multiple `DataFrame`. 

**Acceptable output DataFrame types**

* `DataFrame`, `LocalDataFrame`, `pd.DataFrame`, `List[List[Any]]`, `Iterable[List[Any]]`, `EmptyAwareIterable[List[Any]]`, `List[Dict[str, Any]]`, `Iterable[Dict[str, Any]]`, `EmptyAwareIterable[Dict[str, Any]]`

**Further Notes**

* If the output type is NOT one of Fugue `DataFrame`, `LocalDataFrame` or `pd.DataFrame`, the output schema must be specified because it can't be inferred.
* `ArrayDataFrame` and other local dataframes can't be used as annotation, you must use `LocalDataFrame` or `DataFrame`
* `DataFrame` or `DataFrames` are the recommended input/output types. All other acceptable types are variations of `LocalDataFrame`, which means that the data has to be collected on one machine (the driver) to process.
* `Iterable`-like input may have different execution plans to bring data to driver, in some cases it can be less optimal, you must be careful.

+++

## Native Approach

The native approach is using a regular function without any edits beyond type annotations for both the input dataframes and output. It is converted to a Fugue extension during runtime. In the example below, we have three functions. The first one,`add1`,  has an output type of `pd.DataFrame`, which means that the schema is already known. The second one, `add`, has an output type of `Iterable[Dict[str,Any]]`, which does hold schema so it has to be provided during the `process` call inside `FugueWorkflow`.

Lastly, `concat` shows how to combine multiple DataFrames into one.

```{code-cell} ipython3
from typing import Iterable, Dict, Any, List
import pandas as pd
from fugue import FugueWorkflow

# fugue knows the schema because the output in pd.DataFrame
def add1(df:pd.DataFrame, n=1) -> pd.DataFrame:
    df["b"]+=n
    return df

# schema is not known so it has to be provided later
# in practice, it's rare to use such output type for a processor
def add2(df:List[Dict[str,Any]], n=1) -> Iterable[Dict[str,Any]]:
    for row in df:
        row["b"]+=n
        yield row

def concat(df1:pd.DataFrame, df2:pd.DataFrame) -> pd.DataFrame:
    return pd.concat([df1,df2]).reset_index(drop=True)

with FugueWorkflow() as dag:
    df = dag.df([[0,1],[0,2]],"a:int,b:int")
    df1 = df.process(add1, params={"n":2})
    df2 = df.process(add2, schema="a:int,b:int", params={"n":2})
    dag.process(df1,df2, using=concat).show()
```

It's also important to know how to use `DataFrames` as input annotation. Because this is the only way accept a **dynamic** number of input DataFrames.

```{code-cell} ipython3
from fugue import DataFrames, DataFrame

def concat(dfs:DataFrames) -> pd.DataFrame:
    pdfs = [df.as_pandas() for df in dfs.values()]
    return pd.concat(pdfs).reset_index(drop=True) # Fugue can't take pandas dataframe with special index

with FugueWorkflow() as dag:
    df1 = dag.df([[0,1]],"a:int,b:int")
    df2 = dag.df([[0,2],[1,3]],"a:int,b:int")
    df3 = dag.df([[1,1]],"a:int,b:int")
    dag.process(df1,df2,df3,using=concat).show()
```

## Schema Hint

The schema can also be provided during the function definition through the use of the schema hint comment. Providing it during definition means it does not need to be provided inside the `FugueWorkflow`.

If you are using `DataFrame`, `LocalDataFrame` or `pd.DataFrame` as the output type, schema hints can't be used because the schema will be inferred. Also, the best practice is to use `DataFrame` as the output type when using schema hints.

```{code-cell} ipython3
# schema: a:int, b:int
def add(df:List[Dict[str,Any]], n=1) -> Iterable[Dict[str,Any]]:
    for row in df:
        row["b"]+=n
        yield row

with FugueWorkflow() as dag:
    df = dag.df([[0,1]],"a:int,b:int")
    df.process(add).show()
```

## Decorator Approach

There is no obvious advantage to use the decorator approach for defining a `Processor`. In general, the decorator is good if the schema is too long to type out as a comment in one line or for adding explicitness to code.

```{code-cell} ipython3
from fugue import processor

@processor("a:int, b:int")
def add(df:List[Dict[str,Any]], n=1) -> Iterable[Dict[str,Any]]:
    for row in df:
        row["b"]+=n
        yield row


with FugueWorkflow() as dag:
    dag.df([[0,1]],"a:int,b:int").process(add).show()
```

## Interface Approach (Advanced)

All the previous methods are just wrappers of the interface approach. They cover most of use cases and are simpler to use. But if you want to get all execution context such as partition information, use interface approach.

In the interface approach, type annotations are not necessary but it's good practice to have them.

```{code-cell} ipython3
:tags: [remove-stderr]
from fugue import Processor, DataFrames, DataFrame
from fugue_spark import SparkExecutionEngine


class Partitioner(Processor):
    def process(self, dfs:DataFrames) -> DataFrame:
        assert len(dfs)==1
        engine = self.execution_engine
        partion = self.partition_spec
        return engine.repartition(dfs[0], partition_spec = partion)


with FugueWorkflow(SparkExecutionEngine) as dag:
    df = dag.df([[0,1],[0,3],[1,2],[1,1]],"a:int,b:int")
    # see the output is sorted by b, partition is passed into Partitioner as partition_spec
    df.partition(num=1, presort="b").process(Partitioner).show() 
```

## Using the ExecutionEngine

In some cases, the `Processor` has to be aware of the `ExecutionEngine`. **This is an example of how to write native Spark code inside Fugue.**

```{code-cell} ipython3
from fugue import ExecutionEngine
from fugue_spark import SparkDataFrame

# pay attention to the input and output annotations, 
# the function uses general DataFrame instead of Spark DataFrame
def add(e:ExecutionEngine, df:DataFrame, temp_name="x") -> DataFrame:
    assert isinstance(e,SparkExecutionEngine) # this extension only works with SparkExecutionEngine
    df = e.to_df(df) # to make sure df is SparkDataFrame, or conversion is done here
    df.native.createOrReplaceTempView(temp_name)  # df.native is spark dataframe
    sdf = e.spark_session.sql("select a,b+1 as b from "+temp_name)  # this is how you get spark session
    return SparkDataFrame(sdf) # you must wrap as Fugue SparkDataFrame to return

with FugueWorkflow(SparkExecutionEngine) as dag:
    df = dag.df([[0,1],[0,2],[1,3],[1,1]],"a:int,b:int")
    df.process(add, params={"temp_name":"y"}).show()
```
