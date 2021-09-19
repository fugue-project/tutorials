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

# Outputter

`Outputter` represents a terminal piece of logic in a workflow. Outputter is the only Fugue extension that does not return a DataFrame. It is called Outputter because it is normally used to save data or print on the console. `Outputter` is used on entire dataframes and executes on the driver. Fugue's `save` is an example of an Outputter

In this tutorial are the methods to define an `Outputter`. There is no preferred method and Fugue makes it flexible for users to choose whatever interface works for them. The four ways are native approach, schema hint, decorator, and the class interface in order of simplicity.

## Example Use Cases

* **Pretty printers for console and Jupyter**
* **Writing data to a database**
* **Unit test assertions** can be done by taking in a DataFrame and checking the values.

## Quick Notes on Usage

**ExecutionEngine aware**

* `Outputters` run on the driver so they are aware of the `ExecutionEngine` being used. Passing a parameter with the `ExecutionEngine` annotation will pass in the current `ExecutionEngine`. There is an example of this later.

**Acceptable input DataFrame types**

* `DataFrame`, `LocalDataFrame`, `pd.DataFrame`, `List[List[Any]]`, `Iterable[List[Any]]`, `EmptyAwareIterable[List[Any]]`, `List[Dict[str, Any]]`, `Iterable[Dict[str, Any]]`, `EmptyAwareIterable[Dict[str, Any]]`
* Input can also be Fugue `DataFrames`, which is a collection of Fugue `DataFrame`. 

**Acceptable output DataFrame types**

* `Outputter` can't output anything. The annotation has to be `None`.

**Further notes**

* `ArrayDataFrame` and other local dataframes can't be used as annotation, you must use `LocalDataFrame` or `DataFrame`
* Variations of `LocalDataFrame` will bring the entire dataset onto driver, for an Outputter this might be an expected operation, but you need to be careful.
* `Iterable`-like input may have different exeuction plans to bring data to driver, in some cases it can be less optimial (slower), you need to be careful.

+++

## Native Approach

The native approach is using a regular function without any edits beyond type annotations. You just need to have acceptable type annotations for the input DataFrames and the output annotation should be None.

```{code-cell} ipython3
from typing import Iterable, Dict, Any, List
import pandas as pd
from fugue import FugueWorkflow

def out(df:List[List[Any]], n=1) -> None:
    for i in range(n):
        print(df)

def out2(df1:pd.DataFrame, df2:List[List[Any]]) -> None:
    print(df1)
    print(df2)

with FugueWorkflow() as dag:
    df = dag.df([[0,1],[0,2],[1,3],[1,1]],"a:int,b:int")
    df.output(out, params={"n":2})
    dag.output(df,df,using=out2)
```

It's also important to know how to use `DataFrames` as input annotation. Because this is the only way accept a **dynamic** number of input DataFrames.

```{code-cell} ipython3
from fugue import DataFrames

def out(dfs:DataFrames) -> None:
    for k, v in dfs.items():
        v.show(title=k)

with FugueWorkflow() as dag:
    df1 = dag.df([[0,1]],"a:int,b:int")
    df2 = dag.df([[0,2],[1,3]],"a:int,b:int")
    df3 = dag.df([[1,1]],"a:int,b:int")
    dag.output(df1,df2,df3,using=out)
```

## Schema Hint

The schema hint does not apply to the output of `Outputter` because the output annotation has to be None and there is no DataFrame returned. A schema hint with `schema: None` can be used but it does not do anything.

```{code-cell} ipython3
from fugue import outputter

# schema: None
def out(df:List[List[Any]], n=1) -> None:
    for i in range(n):
        print(df)

with FugueWorkflow() as dag:
    dag.df([[0,1]],"a:int,b:int").output(out)
```

## Decorator Approach

Similar to the schema hint, there is no obvious advantage to use decorator for `Outputter` because there is no output schema so the decorator doesn't do much besides making the code more explicit.

```{code-cell} ipython3
from fugue import outputter

@outputter()
def out(df:List[List[Any]], n=1) -> None:
    for i in range(n):
        print(df)

with FugueWorkflow() as dag:
    dag.df([[0,1]],"a:int,b:int").output(out)
```

## Interface Approach (Advanced)

All the previous methods are just wrappers of the interface approach. They cover most of the use cases and simplify the usage. But if you want to get all execution context such as partition information, use interface.

In the interface approach, type annotations are not necessary, but again, it's good practice to have them.

```{code-cell} ipython3
:tags: [remove-stderr]
from fugue import Outputter
from fugue_spark import SparkExecutionEngine

class Save(Outputter):
    def process(self, dfs:DataFrames) -> None:
        assert len(dfs)==1
        assert isinstance(self.execution_engine, SparkExecutionEngine)
        session = self.execution_engine.spark_session
        # we get the partition information from Outputter
        by = self.partition_spec.partition_by
        df = self.execution_engine.to_df(dfs[0])
        path = self.params.get_or_throw("path",str)
        df.native.write.partitionBy(*by).format("parquet").mode("overwrite").save(path)

with FugueWorkflow(SparkExecutionEngine) as dag:
    df = dag.df([[0,1],[0,3],[1,2],[1,1]],"a:int,b:int")
    df.partition(by=["a"]).output(Save, params=dict(path="/tmp/x.parquet"))
```

## Using the ExecutionEngine

In some cases, the `Outputter` has to be aware of the `ExecutionEngine`. **This is an example of how to write native Spark code inside Fugue.**

```{code-cell} ipython3
from fugue import ExecutionEngine, DataFrame

# pay attention to the input annotations
def out(e:ExecutionEngine, df:DataFrame) -> None:
    assert isinstance(e,SparkExecutionEngine) # this extension only works with SparkExecutionEngine
    df = e.to_df(df) # to make sure df is Spark DataFrame, or conversion is done here
    df.native.show()

with FugueWorkflow(SparkExecutionEngine) as dag:
    df = dag.df([[0,1],[0,2],[1,3],[1,1]],"a:int,b:int")
    df.output(out)
```
