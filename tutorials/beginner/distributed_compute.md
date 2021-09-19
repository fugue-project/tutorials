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

# Distributed Compute

This is a heart of Fugue. In the previous sections, we went over how to use Fugue in the form of extensions and basic data operations such as joins. In this section, we'll talk about how those Fugue extensions scale.

+++

## Partition and Presort

One of the most fundamental distributed compute concepts is the partition. Our data is spread across several machines, and we often need to rearrange the way the data is spread across the machines. This is because of operations that need all of the related data in one place. For example, calculating the median value per group requires all of the data from the same group on one machine. Fugue allows users to control the paritioning scheme during execution.

In the example below, `take` is an operation that extracts `n` number of rows. We apply take on each partition. We will have two partitions because `col1` is the partition key and it only has 2 values.

```{code-cell} ipython3
from fugue import FugueWorkflow
import pandas as pd 

data = pd.DataFrame({'col1':[1,1,1,2,2,2], 'col2':[1,4,5,7,4,2]})
df2 = data.copy()

with FugueWorkflow() as dag:
    df = dag.df(df2)
    df = df.partition(by=['col1'], presort="col2 desc").take(1)
    df.show()
```

We also used `presort`. The presort key here was `col2 desc`, which means that the data is sorted in descending order after partitioning. This makes the `take` operation give us the max value. We'll go over one more example.

```{code-cell} ipython3
# schema: *, col2_diff:int
def diff(df: pd.DataFrame) -> pd.DataFrame:
    df['col2_diff'] = df['col2'].diff(1)
    return df

df2 = data.copy()
with FugueWorkflow() as dag:
    df = dag.df(df2)
    df = df.partition(by=['col1']).transform(diff)
    df.show()
```

Notice there are 2 NULL values in the previous example. This is because the first element of the `diff` operation results in NULL. The reason we have 2 NULLs is because the `transformer` was applied once for each partition. The `partition-transform` semantics are very similar to the `pandas groupby-apply` semantics. There is a deeper dive into partitions in the advanced tutorial.

+++

## CoTransformer

Last section, we skipped the `cotransformer` because it required knowledge about partitions. The `cotransformer` takes in multiple DataFrames that are **partitioned in the same way** and outputs one DataFrame. In order to use a `cotransformer`, the `zip` method has to be used first to join them by their common keys. There is also a `@cotransformer` decorator can be used to define the `cotransformer`, but it will still be invoked by the `zip-transform` syntax.

In the example below, we will do a merge as-of operation on different groups of data. In order to align the data with events as they get distributed across the cluster, we will partition them in the same way.

```{code-cell} ipython3
import pandas as pd

data = pd.DataFrame({'group': (["A"] * 5 + ["B"] * 5),
                     'year': [2015,2016,2017,2018,2019] * 2})

events = pd.DataFrame({'group': ["A", "A", "B", "B"],
                       'year': [2014, 2016, 2014, 2018],
                       "value": [1, 2, 1, 2]})

events.head()
```

The pandas `merge_asof` function requires that the `on` column is sorted. To do this, we apply a `partition` strategy on Fugue by group and presort by the year. By the time it arrives in the `cotransformer`, the dataframes are sorted and grouped.

```{code-cell} ipython3
from fugue import FugueWorkflow

# schema: group:str,year:int,value:int
def merge_asof(data:pd.DataFrame, events:pd.DataFrame) -> pd.DataFrame:
    return pd.merge_asof(data, events, on="year", by="group")

with FugueWorkflow() as dag:
    data = dag.df(data)
    events = dag.df(events)

    data.zip(events, partition={"by": "group", "presort": "year"}).transform(merge_asof).show()
```

In this example, the important part to note is each group uses the pandas `merge_asof` independently. This function is very flexible, allowing users to specify forward and backward merges along with a tolerance. This is tricky to implement well in Spark, but the `cotransformer` lets us do it easily.

This operation was partitioned by the column `group` before the `cotransform` was applied. This was done through the `zip` command. `CoTransform` is a more advanced operation that may take some experience to get used to.

+++

## Persist and Broadcast

Persist and broadcast are two other distributed compute concepts that Fugue has support for. Persist keeps a DataFrame in memory to avoid recomputation. Distributed compute frameworks often need an explicit `persist` call to know which DataFrames need to be kept, otherwise they tend to be calculated repeatedly.

Broadcasting is making a smaller DataFrame available on all the workers of a cluster. Without `broadcast`, these small DataFrames would be repeatedly sent to workers whenever they are needed to perform an operation. Broadcasting caches them on the workers.

```{code-cell} ipython3
with FugueWorkflow() as dag:
    df = dag.df([[0,1],[1,2]],"a:long,b:long")
    df.persist()
    df.broadcast()
```
