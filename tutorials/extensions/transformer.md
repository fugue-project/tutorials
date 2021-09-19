---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.10.3
kernelspec:
  display_name: Python 3
  language: python
  name: python3
---

# Transformer

This is the most used extension in Fugue. The `Transformer` represents the logic unit executing on partitions of the input dataframe. Because the `Transformer` is concerned with the logic on a logical partition level, it is unaware of the `ExecutionEngine` and is executed on the workers as opposed to the driver. Partitioning logic is also not a concern of `Transformer` and should be specified in a previous step.

Fugue's `partition-transform` semantic is similar to the `groupby-apply` semantic of Pandas. The main difference is that the `partition-transform` semantic is scalable to distributed compute as the distribution of logical groups across workers is accounted for. For more information, read about [partitioning](../advanced/partition.ipynb) in Fugue.

In this tutorial are the methods to define an `Transformer`. There is no preferred method and Fugue makes it flexible for users to choose whatever interface works for them. The four ways are native approach, schema hint, decorator, and the class interface in order of simplicity. All of these methods are also compatible with Fugue's `transform` function.

## Example Use Cases

* **Shift and diff for each group in a timeseries**
* **Training seperate ML models for each group of data**
* **Applying different validations for each partition**

## Quick Notes on Usage

**ExecutionEngine unaware**

* `Transformers` are executed on the workers, meaning that they are not unaware of the `ExecutionEngine`.

**Acceptable input DataFrame types**

* `LocalDataFrame`, `pd.DataFrame`, `List[List[Any]]`, `Iterable[List[Any]]`, `EmptyAwareIterable[List[Any]]`, `List[Dict[str, Any]]`, `Iterable[Dict[str, Any]]`, `EmptyAwareIterable[Dict[str, Any]]`

**Acceptable output DataFrame types** 

* `LocalDataFrame`, `pd.DataFrame`, `List[List[Any]]`, `Iterable[List[Any]]`, `EmptyAwareIterable[List[Any]]`, `List[Dict[str, Any]]`, `Iterable[Dict[str, Any]]`, `EmptyAwareIterable[Dict[str, Any]]`

**Further notes**

* Notice that `ArrayDataFrame` and other local dataframes can't be used as annotation, you must use `LocalDataFrame`.
* `Transformer` requires more explicitness on the output schema compared to `Processor`. This is because schema inference on workers is expensive and unreliable. The schema can be specified through schema hint, decorator, or in the Fugue code.
* All valid transformers can be used with Fugue's `transform` in cases where users just want to bring one function to Spark or Dask.

+++

## Native Approach

The native approach is using a regular function without any edits beyond type annotations for both the input dataframes and output. It is converted to a Fugue extension during runtime. Since schema needs to be explicit, the schema needs to be supplied when the `transformer` is used.

The example below also shows how to `partition` a DataFrame before applying a `transformer` on it. This will apply the transformer on each partition.

```{code-cell} ipython3
from typing import Iterable, Dict, Any, List
import pandas as pd
from fugue import FugueWorkflow

def add(df:pd.DataFrame, n=1) -> pd.DataFrame:
    df["b"]+=n
    return df
    
def get_top(df:Iterable[Dict[str,Any]]) -> Iterable[Dict[str,Any]]:
    yield next(df)
    return

with FugueWorkflow() as dag:
    df = dag.df([[0,1],[0,2],[1,3],[1,1]],"a:int,b:int")
    # with out schema hint you have to specify schema in Fugue code
    df = df.transform(add, schema="*").transform(add, schema="*", params=dict(n=2))

    # get smallest b of each partition
    df.partition(by=["a"], presort="b").transform(get_top, schema="*").show()
    # get largest b of each partition
    df.partition(by=["a"], presort="b DESC").transform(get_top, schema="*").show()
```

## Schema Hint

The schema can also be provided during the function definition through the use of the schema hint comment. Providing it during definition means it does not need to be provided inside the `FugueWorkflow`.

```{code-cell} ipython3
# schema: *
def add(df:pd.DataFrame, n=1) -> pd.DataFrame:
    df["b"]+=n
    return df
    
# schema: *
def get_top(df:Iterable[Dict[str,Any]]) -> Iterable[Dict[str,Any]]:
    yield next(df)
    return

with FugueWorkflow() as dag:
    df = dag.df([[0,1],[0,2],[1,3],[1,1]],"a:int,b:int")
    # syntax for setting parameters
    df = df.transform(add).transform(add, params=dict(n=2)) 
    df.partition(by=["a"], presort="b").transform(get_top).show()
```

### Schema Hint Syntax

There is a special syntax for schema only available to `Transformers` Please read [this](https://triad.readthedocs.io/en/latest/api/triad.collections.html#triad.collections.schema.Schema.transform) for detailed syntax, here we only show some examples.

```{code-cell} ipython3
# schema: *,c:int
def with_c(df:pd.DataFrame) -> pd.DataFrame:
    df["c"]=1
    return df

# schema: *-b
def drop_b(df:pd.DataFrame) -> pd.DataFrame:
    return df.drop("b", axis=1)

# schema: *~b,c
def drop_b_c_if_exists(df:pd.DataFrame) -> pd.DataFrame:
    return df.drop(["b","c"], axis=1, errors='ignore')

with FugueWorkflow() as dag:
    df = dag.df([[0,1],[0,2]],"a:int,b:int")
    df = df.transform(with_c)
    df.show()
    df = df.transform(drop_b)
    df.show()
    df = df.transform(drop_b_c_if_exists)
    df.show()
```

## Decorator Approach

The decorator approach also has the special schema syntax and it can also take a function that generates the schema. This can be used to create new column names or types based on transformer parameters.

```{code-cell} ipython3
from fugue import transformer

# df is the zipped DataFrames, **kwargs is the parameters passed in from Fugue
# the syntax below is equivalent to @transformer("*,c:int") 
@transformer(lambda df, **kwargs: df.schema+"c:int") 
def with_c(df:pd.DataFrame) -> pd.DataFrame:
    df["c"]=1
    return df

with FugueWorkflow() as dag:
    df = dag.df([[0,1],[0,2],[1,3],[1,1]],"a:int,b:int")
    df = df.transform(with_c)
    df.show()
```

## Interface Approach (Advanced)

All the previous methods are just wrappers of the interface approach. They cover most of the use cases and simplify the usage. But for certain cases, implementing the interface approach significantly improves performance. Example scenarios to use the interface approach are:

* The output schema needs partition information, such as partition keys, schema, and current values of the keys.
* The transformer has an expensive but common initialization step for processing each logical partition. Initialization should then happen when initialiazing physical partition, meaning it doesn't unnecessarily repeat.

The biggest advantage of interface approach is that you can customize physical partition level initialization, and you have all the up-to-date context variables to use. In the interface approach, type annotations are not necessary, but again, it's good practice to have them.

From here onwards, we will we using a `create_helper` function that creates a random pandas DataFrame for us.

```{code-cell} ipython3
import numpy as np

def create_helper(ct=20) -> pd.DataFrame:
    np.random.seed(0)
    return pd.DataFrame(np.random.randint(0,10,size=(ct, 3)), columns=list('abc'))
```

The following examples focuses on performance comparisons. To see how to use context variables, see the [CoTransfromer example](./cotransformer.ipynb#Interface-Approach). In the example below, pay attention to the `get_output_schema` method and the `on_init` method. The `on_init` calls the `expensive_init` function which just sleeps for the given amount of time. This represents an operation with significant overhead.

```{code-cell} ipython3
from fugue import Transformer, PandasDataFrame, DataFrame, LocalDataFrame
from time import sleep

def expensive_init(sec=5):
    sleep(sec)

class Median(Transformer):
    # this is invoked on driver side
    def get_output_schema(self, df):
        return df.schema + (self.params.get_or_throw("col", str),float)
    
    # on initialization of the physical partition
    def on_init(self, df: DataFrame) -> None:
        self.col = self.params.get_or_throw("col", str)
        expensive_init(self.params.get("sec",0))
        
    def transform(self, df):
        pdf = df.as_pandas()
        pdf[self.col]=pdf["b"].median()
        return PandasDataFrame(pdf)
        

with FugueWorkflow() as dag:
    df = dag.create(create_helper)
    df.partition(by=["a"]).transform(Median, params={"col":"med", "sec": 1}).show(rows=5) 
```

As a side note, this example shows parameters can be retrieved using `self.params.get` or `self.params.get_or_throw`. `self.params` is a dictionary so the `get` method is just the same as accessing a dictionary. `self.params.get_or_throw` throws an error if the param does not match the given type. 

In order to show the benefit of `on_init` we also create another version of the `Median` transformer using the schema hint. This also calls `expensive_init` in that function for each logical partition. Also, in the run function, we set `num=2` to show the effect when using 2 workers. So for `Median` transformer that used the interface, the `expensive_init` will be called at most twice, but for version which used the schema hint, it will be called for more times.

The numbers may be off if you run this on binder, but focus on the difference in magnitude.

```{code-cell} ipython3
:tags: [remove-stderr]
from fugue_spark import SparkExecutionEngine
from timeit import timeit

# schema: *, m:double
def median(df:pd.DataFrame, sec=0) -> pd.DataFrame:
    expensive_init(sec)
    df["m"]=df["b"].median()
    return df

def run(engine, interfaceless, sec):
    with FugueWorkflow(engine) as dag:
        df = dag.create(create_helper)
        if interfaceless:
            df.partition(by=["a"], num=2).transform(median, params={"sec": sec}).show(rows=5)
        else:
            df.partition(by=["a"], num=2).transform(Median, params={"col":"m", "sec": sec}).show(rows=5)
    
engine = SparkExecutionEngine()
print(f"Interfaceless Execution time: {timeit(lambda: run(engine, True, 1), number=1)}")
print(f"Interface Execution time: {timeit(lambda: run(engine, False, 1), number=1)}")
```

Using the `on_init` method tremendously sped up the operation because the `expensive_init` was not unnecessarily repeated.

+++

## Fugue transform

All of these transformers above can be used with the Fugue `transform` function. The `transform` function takes in a function or transformer and applies it to the dataframe immediately. As with other Transformers, schema needs to be explicit to it either needs to be supplied during the transformer definition, or during runtime with the `schema` argument.

The `transform` function is useful for paralellizing one function over Spark or Dask.

```{code-cell} ipython3
# schema not supplied, so it is passed later
def median1(df:pd.DataFrame, sec=0) -> pd.DataFrame:
    df["m"]=df["b"].median()
    return df

# schema: *, m:double
def median2(df:pd.DataFrame, sec=0) -> pd.DataFrame:
    df["m"]=df["b"].median()
    return df
```

These two transformers can then be used. `median1` was written with the `Native Approach`, so schema will be required for the `transform` call below. `median2` on the other hand, uses the schema hint to provide the schema, so it does not need to be provided during runtime.

In both cases below, we pass in the `SparkExecutionEngine`, which converts the initial `df` into a Spark DataFrame and executes the `median` functions in a distributed way. This also returns a Spark DataFrame because compute was run on `SparkExecutionEngine`. It can be converted back to `pandas` using the `toPandas()` method of Spark DataFrames, but this method is only performant for smaller data.

```{code-cell} ipython3
from fugue import transform

# sample pandas DataFrame
df = create_helper()

df1 = transform(df, 
               median, 
               schema="*, m:double",
               engine=SparkExecutionEngine, 
               partition=dict(by="a")
               )

# schema is known for median2
df2 = transform(df, 
               median2, 
               engine=SparkExecutionEngine, 
               partition=dict(by="a")
               )

df1.show(2)
# convert back to pandas
df2.toPandas().head(2)
```
