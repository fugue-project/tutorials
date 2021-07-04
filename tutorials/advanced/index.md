# Tutorial for Advanced Users

Since you already have experience in Spark or distributed computing in general, you may be interested in the extra values Fugue can add.

All questions are welcome in the Slack channel.

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ)

```{toctree}
:hidden:

dag
extensions
useful_config
execution_engine
validation
schema_dataframes
partition
checkpoint
rpc
x-like
```

## 1. Architecture & Hello World

<img src="../_images/architecture.svg" width="500">

Let's take a look at a [Hello World](hello.ipynb) example


## [2. COVID19 Data Exploration](example_covid19.ipynb)
*If you are against SQL, please skip 2 and 3. There is a complete guide on the programming interface.*

Let's firstly see an example how Fugue SQL is used in data exploration. Fugue SQL is a new way to express your end to end workflow. It's almost equivalent to the programming interface but the way you express a workflow will actually affect the way you think. And for many cases, the SQL mindset may help you build pipelines that are more platform and scale agnostic.



## [3. Fugue SQL](sql.ipynb)
The most fun part of Fugue. You can use SQL instead of python to represent the backbone of your workflow, and you can keep you mindset in SQL in most of the time and with the help of python extensions. In this tutorial, we will cover all syntax of Fugue SQL.



## [4. Stock Sentiment Analysis (Preprocessing)](stock_sentiment.ipynb)
A Fugue use case for NLP preprocessing. You can get a sense of how Fugue works, and why we want to use Fugue layer instead of directly using Pandas.



## [5. Execution Graph (DAG) & Programming Interface](dag.ipynb)
A deep dive on the programming interfaces. In this tutorial we will cover most features of the Fugue programming interface.


## [6. Extensions](extensions.ipynb)
From the previous tutorials you have seen plenty of extension examples, here is a complete guide to the Fugue extensions

### [Transformer](transformer.ipynb) (MUST READ)
The most useful and widely used extension

### [CoTransformer](cotransformer.ipynb)
Transform multiple dataframes partitioned in the same way

### [Creator](creator.ipynb)
Generate dataframes for a DAG

### [Processor](processor.ipynb)
Take in one or multiple dataframes and produce a single dataframe as output

### [Outputter](outputter.ipynb)
Take in one or multiple dataframes to do final jobs such as save and print


## 7. Deep Dive
It's time to have a systematic understanding of the Fugue architecture.

### [Fugue Configurations](useful_config.ipynb) (MUST READ)
These configurations can have significant impact on building and running the Fugue workflows.

### [Data Type, Schema & DataFrames](schema_dataframes.ipynb)
Fugue data types and schema are strictly based on [Apache Arrow](https://arrow.apache.org/docs/index.html). Dataframe is an abstract concept with several built-in implementations to adapt to different dataframes. In this tutorial, we will go through the basic APIs and focus on the most common use cases.

### [Partition](partition.ipynb) (MUST READ)
This tutorial is more focused on explaining the basic ideas of data partitioning. It's less related with Fugue. To have a good understanding of partition is the key for writing high performance code.

### [Checkpoint](checkpoint.ipynb)
Checkpoint is important for advanced users to keep the executions robust and stateful. This section gives you a bigger picture of the checkpoint concept and compared the implementation difference between Fugue and Spark.

### [Execution Engine](execution_engine.ipynb)
The heart of Fugue. It is the layer that unifies many of the core concepts of distributed computing, and separates the underlying computing frameworks from user level logic. Normally you don't directly interact with execution engines. But it's good to understand some basics.

### [Callbacks From Transformers To Driver](rpc.ipynb)
You can provide a callback function to any transformer, to communicate with driver while running

### [X-Like Objects Initialization](x-like.ipynb)
You may often see -like objects in Fugue API document, here is a complete list of these objects and their ways to initialize.