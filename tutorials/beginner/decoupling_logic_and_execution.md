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

# Decoupling Logic and Execution

In the last section, we used Fugue's transform function to port pandas code to Spark. Decoupling logic and execution is one of the primary motivations of Fugue. When transitioning a project from pandas to Spark, the majority of the code normally has has to be re-written. This is because using either pandas or Spark makes code highly coupled with that framework. This leads to a couple of problems:

1. Users have to learn an entirely new framework to work with distributed compute problems
2. Logic written for a *small data* project does not become reusable for a *big data* project
3. Testing becomes a heavyweight process for distributed compute, especially Spark
4. Along with number 3, iterations for distributed compute problems become slower and more expensive

Fugue believes that code should minimize dependency on frameworks as much as possible. This provides flexibility and portability. **By decoupling logic and execution, we can focus on our logic in a scale-agnostic way, and then choose which engine to use when the time arises.**

+++

## Differences between Pandas and Spark

To illustrate the first two main points above, we'll use a simple example. For the data below, we are interested in getting the first three digits of the `phone` column and populating a new column called `location` by using a dictionary that maps the values. We start by preparing the sample data and defining the mapping.

```{code-cell} ipython3
import pandas as pd

_area_code_map = {"217": "Champaign, IL", "407": "Orlando, FL", "510": "Fremont, CA"}

data = pd.DataFrame({"phone": ["(217)-123-4567", "(217)-234-5678", "(407)-123-4567", 
                               "(407)-234-5678", "(510)-123-4567"]})
data.head()
```

First, we'll perform the operation in pandas. It's very simple because of the `.map()` method in pandas

```{code-cell} ipython3
def map_phone_to_location(df: pd.DataFrame) -> pd.DataFrame:
    df["location"] = df["phone"].str.slice(1,4).map(_area_code_map)
    return df

map_phone_to_location(data.copy())
```

Next we'll perform the same operation in Spark and see how different the syntax is.

```{code-cell} ipython3
# Setting up Spark session
from pyspark.sql import SparkSession, DataFrame
spark = SparkSession.builder.getOrCreate()
```

```{code-cell} ipython3
from pyspark.sql.functions import create_map, col, lit, substring
from itertools import chain

df = spark.createDataFrame(data)  # converting the previous Pandas DataFrame

mapping_expr = create_map([lit(x) for x in chain(*_area_code_map.items())])

def map_phone_to_location(df: DataFrame) -> DataFrame:
    _df = df.withColumn("location", mapping_expr[substring(col("phone"),2,3)])
    return _df

map_phone_to_location(df).show()
```

Looking at the two code examples, we had to reimplement the exact same functionality with completely different syntax. This isn't a cherry-picked example. Data practitioners will often have to write two implementations of the same logic, one for each framework, especially as the logic gets more specific. 

This is where Fugue comes in. Users can use the abstraction layer to only write one implementation of the function. This can then be applied to pandas, Spark, and Dask. All we need to do is apply a `transformer` decorator to the pandas implementation of the function. The decorator takes in a string that specifies the output schema. The `transform` function does the same thing to the function that is passed in.

```{code-cell} ipython3
from fugue import transformer

@transformer("*, location:str")
def map_phone_to_location(df: pd.DataFrame) -> pd.DataFrame:
    df["location"] = df["phone"].str.slice(1,4).map(_area_code_map)
    return df
```

By wrapping the function with the decorator, we can then use it inside a `FugueWorkflow`. The `FugueWorkflow` constructs a directed-acyclic graph (DAG) where the inputs and outputs are DataFrames. More details will follow in the next sections but the important thing for now is to show how it's used. The code block below is still running in Pandas.

```{code-cell} ipython3
from fugue import FugueWorkflow

with FugueWorkflow() as dag:
    df = dag.df(data.copy())  # Still the original Pandas DataFrame
    df = df.transform(map_phone_to_location)
    df.show()
```

In order to bring it to Spark, all we need to do is pass the `SparkExecutionEngine` into `FugueWorkflow`, similar to how we used the `transform` function to Spark in the last section. Now all the code underneath the `with` statement will run on Spark. We did not make any modifications to `map_phone_to_location` in order to bring it to Spark. By wrapping the function with a `transformer`, it became agnostic to the ExecutionEngine it was operating on. We can use the same function in Spark or Dask without making modifications.

```{code-cell} ipython3
from fugue_spark import SparkExecutionEngine

with FugueWorkflow(SparkExecutionEngine) as dag:
    df = dag.df(data.copy())  # Still the original Pandas DataFrame
    df = df.transform(map_phone_to_location)
    df.show()
```

## `transform` versus `FugueWorkflow`

We have seen the two approaches to bring Python and pandas code to Spark with Fugue. The `transform` function introduced in the first section allows users to leave a function in pandas or Python, and then port it to Spark and Dask. Meanwhile, `FugueWorkflow` does the same for full workflows as opposed to one function.

For example, if we had five different functions to call `transform` on and bring to Spark, we would need to specify the `SparkExecutionEngine` five times. The `FugueWorkflow` allows us to make the entire computation run on either pandas, Spark, or Dask. Both are similar in principle, in that they leave the original functions decoupled to the execution environment.

+++

## Independence from Frameworks

We earlier said that the abstraction layer Fugue provides makes code independent of any framework. To show this is true, we can actually rewrite the `map_phone_to_location` function in native Python and still apply it on the pandas and Spark engines.

Below is the implementation in native Python. Similar to earlier, we are running this on Spark by passing in the `SparkExecutionEngine`. A function written in native Python can be ported to pandas, Spark, and Dask.

```{code-cell} ipython3
from typing import List, Dict, Any

# schema: *, location:str
def map_phone_to_location(df: List[Dict[str,Any]]) -> List[Dict[str,Any]]:
    for row in df:
        row["location"] = _area_code_map[row["phone"][1:4]]
    return df

with FugueWorkflow(SparkExecutionEngine) as dag:
    df = dag.df(data.copy())  # Still the original Pandas DataFrame
    df = df.transform(map_phone_to_location)
    df.show()
```

Notice the `@transformer` decorator was removed from `map_phone_to_location`. Instead, it was replaced with a comment that specified the schema. Fugue reads in this comment as the **schema hint**. Now, this function is truly independent of any framework and written in native Python. **It is even independent from Fugue itself.** Fugue only appears when we reach the execution part of the code. The logic, however, is not coupled with any framework. The type annotations in the `map_phone_to_location` caused the DataFrame to be converted as it was used by the function. If users want to offboard from Fugue, they can use their function with Pandas `apply` or Spark user-defined functions (UDFs).

Is the native Python implementation or Pandas implementation of `map_phone_to_location` better? Is the native Spark implementation better? 

The main concern of Fugue is clear readable code. **Users can write code in whatever expresses their logic the best**. The compute efficiency lost by using Fugue is unlikely to be significant, especially in comparison to the developer efficiency gained through more rapid iterations and easier maintenance. In fact, Fugue is designed in a way that often sees speed ups compared to inexperienced users working with native Spark code. Fugue handles a lot of the tricks necessary to use Spark effectively. 

Fugue also future-proofs the code. If one day Spark and Dask are replaced by a more efficient framework, a new ExecutionEngine can be added to Fugue to support that new framework.

+++

## Testability and Maintainability

Fugue code becomes easily testable because the function contains logic that is portable across all pandas, Spark, and Dask. All we have to do is run some values through the defined function. We can test code without the need to spin up compute resources (such as Spark or Dask clusters). This hardware often takes time to spin up just for a simple test, making it painful to run unit tests on Spark. Now, we can test quickly with native Python or pandas, and then execute on Spark when needed. Developers that use Fugue benefit from more rapid iterations in their data projects.

```{code-cell} ipython3
# Remember the input was List[Dict[str,Any]]
map_phone_to_location([{'phone': '(407)-234-5678'}, 
                       {'phone': '(407)-234-5679'}])
```

Even if the output here is a `List[Dict[str,Any]]`, Fugue takes care of converting it back to a DataFrame.

+++

## Fugue as a Mindset

Fugue is a framework, but more importantly, it is a mindset. 

1. Fugue believes that the framework should adapt to the user, not the other way around
2. Fugue lets users code express logic in a scale-agnostic way, with the tools they prefer
3. Fugue values readability and maintainability of code over deep framework-specific optimizations

Using distributed computing is currently harder than it needs to be. However, these systems often follow similar patterns, which have been abstracted to create a framework that lets users focus on defining their logic. We cover these concepts in the rest of tutorials. If you're new to distributed computing, Fugue is the perfect place to get started.

## [Optional] Comparison to Modin and Koalas

Fugue gets compared a lot of Modin and Koalas. Modin is a pandas interface for execution on Dask, and Koalas is a pandas interface for execution on Spark. Fugue, Modin, and Koalas have similar goals in making an easier distributed computing experience. The main difference is that Modin and Koalas use pandas as the grammar for distributed compute. Fugue, on the other hand, uses native Python and SQL as the grammar for distributed compute (though pandas is also supported). 

The clearest example of pandas not being compatible with Spark is the acceptance of mixed-typed columns. A single column can have numeric and string values. Spark on the other hand, is strongly typed and enforces the schema. More than that, pandas is strongly reliant on the index for operations. As users transition to Spark, the index mindset does not hold as well. Order is not always guaranteed in a distributed system, there is an overhead to maintain a global index and moreover it is often not necessary.
