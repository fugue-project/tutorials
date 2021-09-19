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

# Creator

`Creator` represents the logic unit to generate a DataFrame. It is used at the start of workflows. The built-in `load` of Fugue is an example of a Creator.

In this tutorial are the methods to define a `Creator`. There is no preferred method and Fugue makes it flexible for users to choose whatever interface works for them. The four ways are native approach, schema hint, decorator, and the class interface in order of simplicity.

## Example Use Cases

* **Reading special data sources** like constructing a DataFrame using an API.
* **Querying a database** using `pyodbc` and returning a DataFrame
* **Create mock data for unit tests**.

+++

## Quick Notes on Usage

**ExecutionEngine aware**

* Creators run on the driver so they are aware of the `ExecutionEngine` being used. Passing a parameter with the `ExecutionEngine` annotation will pass in the current `ExecutionEngine`. There is an example of this later.

**Acceptable input DataFrame types**

* `Creator` can't take DataFrames in, but can take other parameters.

**Acceptable output DataFrame types**

* `DataFrame`, `LocalDataFrame`, `pd.DataFrame`, `List[List[Any]]`, `Iterable[List[Any]]`, `EmptyAwareIterable[List[Any]]`, `List[Dict[str, Any]]`, `Iterable[Dict[str, Any]]`, `EmptyAwareIterable[Dict[str, Any]]`

**Further notes**

* If the output type is NOT one of Fugue `DataFrame`, `LocalDataFrame` or `pd.DataFrame`, the output schema must be specified because it can't be inferred.

+++

## Native Approach

The native approach is using a regular function without any edits beyond type annotations. It is converted to a Fugue extension during runtime. In the example below, we have two create functions. The first one has an output type of `pd.DataFrame`, which means that the schema is already known. The second one has an output type of `List[List[Any]]`, which does hold schema so it has to be provided during the `create` call inside `FugueWorkflow`.

```{code-cell} ipython3
from typing import Iterable, Dict, Any, List
import pandas as pd
from fugue import FugueWorkflow

# fugue knows the schema because the output in pd.DataFrame
def create1(n=1) -> pd.DataFrame:
    return pd.DataFrame([[n]],columns=["a"])

# schema is not known so it has to be provided later
def create2(n=1) -> List[List[Any]]:
    return [[n]]

with FugueWorkflow() as dag:
    dag.create(create1, params={"n":2}).show()
    dag.create(create2, schema="a:int", params={"n":2}).show()
```

## Schema Hint

The schema can also be provided during the function definition through the use of the schema hint comment. Providing it during definition means it does not need to be provided inside the `FugueWorkflow`.

```{code-cell} ipython3
# schema: a:int
def create2(n=1) -> List[List[Any]]:
    return [[n]]

with FugueWorkflow() as dag:
    dag.create(create2).show()
```

## Decorator Approach

There is no obvious advantage to use the decorator approach for defining a `Creator`. In general, the decorator is good if the schema is too long to type out as a comment in one line. 

```{code-cell} ipython3
from fugue import creator

@creator("a:int")
def create(n=1) -> List[List[Any]]:
    return [[n]]

with FugueWorkflow() as dag:
    dag.create(create).show()
```

## Interface Approach (Advanced)

All the previous methods are just wrappers of the interface approach. They cover most of use cases and are simpler to use. But if you want to get all execution context such as partition information, use interface approach.

In the interface approach, type annotations are not necessary but it's good practice to have them.

```{code-cell} ipython3
from fugue import Creator, DataFrame

class Array(Creator):
    def create(self) -> DataFrame:
        engine = self.execution_engine
        n = self.params.get_or_throw("n",int)
        return engine.to_df([[n]],"a:int")


with FugueWorkflow() as dag:
    dag.create(Array, params=dict(n=1)).show()
```

## Using the ExecutionEngine

In some cases, the `Creator` has to be aware of the `ExecutionEngine`. **This is an example of how to write native Spark code inside Fugue.**

```{code-cell} ipython3
:tags: [remove-stderr]
from fugue import ExecutionEngine
from fugue_spark import SparkExecutionEngine, SparkDataFrame

# pay attention to the input and output annotations, they are both general DataFrame
def create(e:ExecutionEngine, n=1) -> DataFrame:
    assert isinstance(e,SparkExecutionEngine) # this extension only works with SparkExecutionEngine
    sdf= e.spark_session.createDataFrame([[n]], schema="a:int")  # this is how you get spark session
    return SparkDataFrame(sdf) # you must wrap as Fugue SparkDataFrame to return

with FugueWorkflow(SparkExecutionEngine) as dag:
    dag.create(create, params={"n":2}).show()
```
