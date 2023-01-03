# Extensions

All questions are welcome in the Slack channel.

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)

We have previously introduced extensions in the [Getting Started section](../beginner/beginner_extension.ipynb). This section is a more comprehensive guide to extensions in Fugue. Extensions are user-created functions that perform operations on DataFrames. By converting these functions to the approriate extension, they can be brought into Fugue workflows. 

![](../../images/extensions.svg)

The construction of workflow and the construction of ExecutionEngines can be totally decoupled. But you can also couple them, this may be useful when you need to use certain config in an ExecutionEngine. 

In fugue, workflow is static, it is strictly the description or **spec** of your logic flow, and it has nothing to do with execution. When you finish writing the workflow spec, you can also choose a Fugue ExecutionEngine and an Adagio workflow ExecutionEngine for the end to end execution. The Adagio workflow ExecutionEngine will turn your workflow _spec_ into a workflow _instance_ (nodes specs will also turn to node instances) and execute in certain order. For example Adagio default ExecutionEngine `SequentialExecutionEngine` will run everything sequentially in the order you list in the workflow spec. In each node of the execution graph, it will use the given Fugue ExecutionEngine. And the edges of the graph is always `DataFrame`


## Graph Nodes - Extensions

There are only 3 types of nodes in Fugue workflow. They are also called driver side extensions.

![](../../images/nodes.svg)

* **Creator**: no input, single output dataframe, it is to produce dataframe input for other types of nodes, for example load file or create mock data
* **Processor**: one or multiple input dataframes, single output dataframe, it is to do certain transformation and pass to the next node
* **Outputter**: one or multiple input dataframes, no input, it is to finalize the process of the input, for example save or print

All these nodes/extensions are executed on driver side and they are ExecutionEngine aware. For example if you really want to use certain features of RDD, you can write your native Spark code in a `Processor` because inside a processor, you can access the ExecutionEngine and if it is SparkExecutionEngine, you can get `spark_session`. There will be examples in this tutorial.

There are two special types of `Processor`: `Transformer` and `CoTransformer`. They are special because they are NOT ExeuctionEngine aware, and their exposed interface is to describe the job to do on worker side not driver side.

![](../../images/transformers.svg)

* [**Transformer**](./transformer.ipynb): single `LocalDataFrame` in, single `LocalDataFrame` out
* [**CoTransformer**](./cotransformer.ipynb): one or multiple `LocalDataFrame` in, single `LocaDataFrame` out
* [**OutputTransformer**](./transformer.ipynb#Output-Transformer): single `LocalDataFrame` in, no output
* [**OutputCoTransformer**](./cotransformer.ipynb#Output-CoTransformer): one or multiple `LocalDataFrame` in, no output

They only care about how to process a partition of the dataframe(s) on a local machine.

| . | Creator | Processor | Outputter | Transformer | CoTransformer | OutputTransformer | OutputCoTransformer
|---|---|---|---|---|---|---|---
|Input | 0    | 1+        | 1+        | 1           | 1+            | 1                 | 1+
|Output| 1    | 1         | 0         | 1           | 1             | 0                 | 0
|Side  |Driver|Driver     | Driver    | Worker      | Worker        | Worker            | Worker
|Engine Aware | Yes | Yes | Yes       | No          | No            | No                | No

Fugue orchestrates its extensions in Fugue Workflows

## Driver vs Worker

To fully understand extensions, it is important to understand the basic distributed compute architecture. A compute cluster is comprised of one `driver`, and multiple `worker` nodes. The `driver` is the machine responsible for orchestrating the `worker` machines. Because the driver is responsible for communciating and distributing work, it has access to information that worker nodes don't have. For example, it keeps track of partition information.

Because of this distinction in distributed computing, there will be code written that is specifically meant to be run on the `driver` machine. These are things like loading and saving a dataframe, or maybe the logic to divide the data into different partitions. On the other hand, code that runs on `workers` will be agnostic to what is happening on other workers.

Note that this means using a function to get the maximum value of a column behaves differently dependending if it is a client-side or worker-side extension. The client-side extension will give a global maximum, while the worker-side extension will give a local maximum.

## Extension Types

In the following descriptions, note the difference between `DataFrame` and `LocalDataFrame`. A `LocalDataFrame` is a dataframe that exists on a single machine. The `LocalDataFrame` is an abstraction for structures like `pd.DataFrame` or `List[List[Any]]`. On the other hand, the `DataFrame` is a dataframe that can exist on multiple machines. Fugue's `DataFrame` class is a abstract version of Spark or Dask DataFrames. `DataFrame` can only be used on driver.

**Driver-side extensions**

* [**Creator**](./creator.ipynb): no input, single output `DataFrame`, it is to produce a `DataFrame` to be used by other extensions
* [**Processor**](./processor.ipynb): one or multiple input `DataFrame`, single output `DataFrame`, it is to do certain transformation and pass to the next node
* [**Outputter**](./outputter.ipynb): one or multiple input `DataFrames`, no input, it is to finalize the process of the input, for example save or print

**Worker-side extensions**

* [**Transformer**](./transformer.ipynb): single `LocalDataFrame` in, single `LocalDataFrame` out
* [**CoTransformer**](./cotransformer.ipynb): one or multiple `LocalDataFrame` in, single `LocaDataFrame` out

**Advanced worker-side extensions**
* [**OutputTransformer**](./outputtransformer.ipynb): single `LocalDataFrame` in, no output
* [**OutputCoTransformer**](./outputcotransformer.ipynb): one or multiple `LocalDataFrame` in, no output

## [Interfaceless](./interfaceless.ipynb)

These extensions can be defined with the appropriate Python class or decorator. For example, a `transformer` can be defined with the `Transformer` class or by using the `@transformer` decorator with a Python function. These are **interfaces** provided by Fugue, but they are not required to convert functions to extensions. As seen in the beginner tutorial, schema hints can be used to define extensions. For example, the following function will create a new column called `c`. 

```python
# schema: *,c:int
def add_transformer(df:pd.DataFrame) -> pd.DataFrame:
    df['c'] = df['a'] + df['b']
    return df
```

This schema hint comment is read by Fugue to make the `add_transformer` an extension during runtime. In fact, the schema hint is not even required if the schema is provided during runtime as seen below. 

```python
from fugue import transform

def add_transformer(df:pd.DataFrame) -> pd.DataFrame:
    df['c'] = df['a'] + df['b']
    return df

df = transform(df, add_transformer, schema="*, c:int")
```

These approaches that leave the code in native Python are called the [interfaceless](./interfaceless.ipynb) approach and are the easiest way to use Fugue. They are designed to be non-invasive, and at the same time, encourage more maintainable code by working around typo annotations and comments. The resulting code is not tied to Fugue, and can run independently.

## Output Schema Requirement

There is a distinction when it comes to `driver`-side and `worker`-side extensions. The driver-side extensions (Creator, Processor, Outputter) have access to the schema, so there is no need to infer or guess it. This is why Fugue does not require the schema to be specified if the output annotation is one of `LocalDataFrame`, `DataFrame`, or `pd.DataFrame` for the `Creator`, `Processor`, and `Outputter`. 

For the `worker`-side extensions, things need to be a bit more explicit. Normally distributed computing frameworks can infer output schema, however, it is neither reliable nor efficient. To infer the schema, it has to go through at least one partition of data and figure out the possible schema. However, `transformers` can produce inconsistent schemas on different partitions. The inference can also take a long time or directly fail. So to avoid potential correctness and performance issues, `Transformer` and `CoTransformer` output schemas are required in Fugue.

```{toctree}
:hidden:

creator
processor
outputter
transformer
cotransformer
outputtransformer
outputcotransformer
interfaceless
```