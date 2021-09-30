# Deep Dive

All questions are welcome in the Slack channel.

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ)

This section is not needed to create end-to-end workflows with Fugue, but it will help give a better understanding of the features available. In some cases, applying these concepts may significantly improve performance.

Since you already have experience in Spark or distributed computing in general, you may be interested in the extra values Fugue can add. 

```{toctree}
:hidden:

dag
useful_config
execution_engine
validation
schema_dataframes
partition
checkpoint
rpc
x-like
```

## Architecture

![](../../images/architecture.svg)

## [Execution Graph (DAG)](dag.ipynb)

Here we discuss the execution graph behind Fugue workflows.


## [Fugue Configurations](useful_config.ipynb) (MUST READ)
These configurations can have significant impact on building and running the Fugue workflows.

## [Execution Engine](execution_engine.ipynb)
The heart of Fugue. It is the layer that unifies many of the core concepts of distributed computing, and separates the underlying computing frameworks from user level logic. Normally you don't directly interact with execution engines. But it's good to understand some basics.

## [Validation](validation.ipynb)
Fugue applies input validation.

## [Data Type, Schema & DataFrames](schema_dataframes.ipynb)
Fugue data types and schema are strictly based on [Apache Arrow](https://arrow.apache.org/docs/index.html). Dataframe is an abstract concept with several built-in implementations to adapt to different dataframes. In this tutorial, we will go through the basic APIs and focus on the most common use cases.

## [Partition](partition.ipynb) (MUST READ)
This tutorial is more focused on explaining the basic ideas of data partitioning. It's less related with Fugue. To have a good understanding of partition is the key for writing high performance code.

## [Checkpoint](checkpoint.ipynb)
Checkpoint is important for advanced users to keep the executions robust and stateful. This section gives you a bigger picture of the checkpoint concept and compared the implementation difference between Fugue and Spark.

## [Callbacks From Transformers To Driver](rpc.ipynb)
You can provide a callback function to any transformer, to communicate with driver while running

## [X-Like Objects Initialization](x-like.ipynb)
You may often see -like objects in Fugue API document, here is a complete list of these objects and their ways to initialize.