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

# Fugue Interface

In the previous section, we talked about the motivation of Fugue and showed a simple example of Fugue code. Here we provide more details around the Fugue syntax that will be needed in actual applications. We will also talk about some of the underlying concepts that were shown in the previous section, but not really explained.

## The Directed Acyclic Graph (DAG)

In the last section, we had a code block that used `with FugueWorkflow() as dag:`. We'll make another simple example below. Here we have a simple `transformer` that takes in two columns and adds them to create a third column called `col3`. This is then used in the `FugueWorkflow`.

```{code-cell} ipython3
import pandas as pd
from fugue import FugueWorkflow

data = pd.DataFrame({'col1': [1,2,3], 'col2':[2,3,4]})
data2 = data.copy()

# schema: *, col3:int
def make_new_col(df: pd.DataFrame) -> pd.DataFrame:
    df['col3'] = df['col1'] + df['col2']
    return df 

with FugueWorkflow() as dag:
    df = dag.df(data2)
    df = df.transform(make_new_col)
    df.show()
```

Nothing here is new. This should all be familiar last section, however we did not really dive into what the `FugueWorkflow` actually does. The `FugueWorkflow` is responsible for constructing a Directed Acyclic Graph, also called a DAG. A lot of people associate the DAG concept with workflow orchestration tools like Airflow, Prefect, of Dagster. While these tools also use DAGs, they use it in a different way than the distributed compute frameworks (Spark and Dask). For orchestration frameworks, the DAG is used to manage dependencies of scheduled tasks. For compute frameworks, the DAG represents a computation graph that is built, validated, and then executed. DAGs are used because distributed compute operations are very expensive and have a lot of room to be optimized. Also, mistakes in a distributed setting are very expensive.

Fugue follows these distributed compute frameworks in using the DAG for validation before execution. DAGs can catch errors significantly earlier, similar to compiling the compute job. For Fugue specifically, the built DAG validates schema, as well as provides the basis for further optimizations. For example, Fugue can detect which DataFrames are re-used in the computation graph, and then persist them automatically to avoid recomputation. The DAG is a graph where the nodes are DataFrames connected by Fugue extensions. We already introduced the most common extension, which is the `transformer`. Schema is tracked throughout the DAG. More extensions will be introduced later.

+++

## Schema

This leads us to schema.  Schema is explicit in Fugue for a couple of reasons. First, is to allow quick validation if the computation job contains all the necessary columns. 

Beyond this, explicit schema is very important in a distributed setting. The DataFrame can be split across multiple machines, and each machine only sees the data it holds. A strong schema guarantees that operations are performed as expected across all machines. A lot of heavy pandas users are not used to explicit schema but it becomes more important in a distributed setting. Inferring schema is an expensive operation. There are 3 basic ways to define schema with Fugue. Note that we are specifying the output schema of the extension.

+++

**Method 1: Schema Hint**

In this case the comment is read and enforced by `FugueWorkflow`. This is the least invasive to code, and is not even dependent on Fugue. If a user chooses to move away from Fugue, these are still helpful comments that can remain in the code.

```{code-cell} ipython3
# schema: *, col3:int
def make_new_col(df: pd.DataFrame) -> pd.DataFrame:
    df['col3'] = df['col1'] + df['col2']
    return df 
```

**Method 2: Decorator**

One of the limitations of the schema hint is that linters often complain if there is a very long schema (past 70 or 80 characters). In that situation, users can import a long string into their script and pass it to the `transformer` decorator. This is also more explicit that this function is being wrapped into a Fugue transformer.

```{code-cell} ipython3
from fugue import transformer

@transformer(schema="*, col3:int")
def make_new_col(df: pd.DataFrame) -> pd.DataFrame:
    df['col3'] = df['col1'] + df['col2']
    return df 
```

**Method 3: FugueWorkflow**

If users don't want to use any of the above options, they can provide the schema in the `transform` call under the FugueWorkflow context manager like the example below. This converts the function to a `transformer` during execution. This leaves the original function untouched and only brings it to Spark or Dask during runtime when needed.

```{code-cell} ipython3
data2 = data.copy()

def make_new_col(df: pd.DataFrame) -> pd.DataFrame:
    df['col3'] = df['col1'] + df['col2']
    return df 
    
with FugueWorkflow() as dag:
    df = dag.df(data2)
    df = df.transform(make_new_col, schema="*, col3:int")
    df.show()
```

## Passing Parameters

We saw in the first section how to pass parameters into the `transform` function but we haven't seen how to do this yet with `FugueWorkflow`. Passing parameters is identical in both approaches. We pass them as a dictionary when we use the `transform` method. 

```{code-cell} ipython3
data2 = data.copy()

# schema: *, col3:int
def make_new_col(df: pd.DataFrame, n=1) -> pd.DataFrame:
    df['col3'] = df['col1'] + df['col2'] + n
    return df 
    
with FugueWorkflow() as dag:
    df = dag.df(data2)
    df = df.transform(make_new_col, params={'n': 10})  # Pass parameters
    df.show()
```

## Loading and Saving Data

Load and save operations are done inside the `FugueWorkflow` and use the appropriate saver/loader for the file extension (.csv, .json, .parquet, .avro) and ExecutionEngine (Pandas, Spark, or Dask). For distributed compute, parquet and avro tend to be the most used because of compression. 

```{code-cell} ipython3
with FugueWorkflow() as dag:
    df = dag.df(data2)
    df.save('/tmp/data.parquet', mode="overwrite", single=True)
    df.save("/tmp/data.csv", mode="overwrite", header=True)
    df2 = dag.load('/tmp/data.parquet')
    df3 = dag.load("/tmp/data.csv", header=True, columns="col1:int,col2:int")
    df3.show()
```

## Summary

In this section we covered some conepts such as the DAG and why explicit schema is needed. We also covered how to define schema and pass in parameters. Combined with loading and saving of files, users can already start using Fugue for working with data.
