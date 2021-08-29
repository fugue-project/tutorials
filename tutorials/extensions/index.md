# Extensions

All questions are welcome in the Slack channel.

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ)

We have previously introduced extensions in the [Getting Started section](../beginner/beginner_extension.ipynb). This section is a more comprehensive guide to extensions in Fugue. Extensions are user-created functions that perform operations on DataFrames. By converting these functions to the approriate extension, they can be brought into Fugue workflows. 

<img src="../../_images/extensions.svg" width="700">

## Extension Types

In the following descriptions, note the difference between `DataFrame` and `LocalDataFrame`. A `LocalDataFrame` is a dataframe that exists on a single machine. This would be something like `pd.DataFrame` or `List[List[Any]]`. On the other hand, the `DataFrame` is a dataframe that can exist on multiple machines. This would be like a Spark or Dask DataFrame.

* [**Creator**](./creator.ipynb): no input, single output `DataFrame`, it is to produce `DataFrame` input for other types of nodes, for example load file or create mock data
* [**Processor**](./processor.ipynb): one or multiple input `DataFrame`, single output `DataFrame`, it is to do certain transformation and pass to the next node
* [**Outputter**](./outputter.ipynb): one or multiple input `DataFrames`, no input, it is to finalize the process of the input, for example save or print
* [**Transformer**](./transformer.ipynb): single `LocalDataFrame` in, single `LocalDataFrame` out
* [**CoTransformer**](./cotransformer.ipynb): one or multiple `LocalDataFrame` in, single `LocaDataFrame` out

**Advanced**
* [**OutputTransformer**](./outtransformer.ipynb): single `LocalDataFrame` in, no output
* [**OutputCoTransformer**](./cotransformer.ipynb#Output-CoTransformer): one or multiple `LocalDataFrame` in, no output

## [Interfaceless](./interfaceless.ipynb)

These extensions can be defined with the appropriate Python class or decorator. For example, a `transformer` can be defined with the `Transformer` class or by using the `@transformer` decorator with a Python function. These are interfaces provided by Fugue, but they are not required to convert functions to extensions. As seen in the beginner tutorial, schema hints can be used to define extensions. For example, the following function will create a new column called `c`. 

```python
# schema: *,c:int
def add_transformer(df:pd.DataFrame) -> pd.DataFrame:
    df['c'] = df['a'] + df['b']
    return df
```

This schema hint comment is read by Fugue to make the `add_transformer` an extension during runtime. In fact, the schema hint is not even required if the schema is provided during runtime as seen below. These approaches that leave the code in native Python are called the [interfaceless](./interfaceless.ipynb) approach. 

```python
from fugue import transform

def add_transformer(df:pd.DataFrame) -> pd.DataFrame:
    df['c'] = df['a'] + df['b']
    return df

df = transform(df, add_transformer, schema="*, c:int")
```

## Why Explicit on Output Schema?

Normally computing frameworks can infer output schema, however, it is neither reliable nor efficient. To infer the schema, it has to go through at least one partition of data and figure out the possible schema. However, what if a transformer is producing inconsistent schemas on different data partitions? What if that partition takes a long time or fail? So to avoid potential correctness and performance issues, `Transformer` and `CoTransformer` output schemas are required in Fugue.

```{toctree}
:hidden:

extensions
creator
processor
outputter
transformer
cotransformer
outtransformer
interfaceless
```