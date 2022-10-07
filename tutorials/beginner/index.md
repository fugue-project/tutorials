# Getting Started

Have questions? Chat with us on Github or Slack:

[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)

Fugue is an abstraction layer that lets users write code in native Python or Pandas and then port it over to Spark and Dask. This section will cover the motivation of Fugue, the benefits of using an abstraction layer, and how to get started. This section is not a complete reference but will be sufficient to get started with writing full workflows in Fugue.


```{toctree}
:hidden:

introduction
type_hinting
schema
partitioning
execution_engine
workflows
joins
extensions
distributed_compute
beginner_sql
```


## [Introduction](introduction.ipynb)
We'll get started by introducing Fugue and the simplest way to use it with the `transform()` function. The `transform()` function can take in a Python or Pandas function and scale it out in Spark or Dask without having to modify it. This provides a very simple interface to parallelize Python and Pandas code on distributed computing engines.

## [Type Hinting](type_hinting.ipynb)
After seeing an example of the `transform()` function, we look into the further flexibility Fugue provides by accepting functions with different input and output types. This allows users to define their logic in whatever expression makes the most sense and bring native Python functions to Spark, Dask or Ray. Having flexibility is important because distributed computing often goes beyond the scope of processing Pandas-like DataFrames. Think of aggregating API calls or processing image data.

## [Schema](schema.ipynb)
After seeing an example of the `transform()` function, we look into the further flexibility Fugue provides by accepting functions with different input and output types. This allows users to define their logic in whatever expression makes the most sense and bring native Python functions to Spark or Dask.

## [Partitioning](partitioning.ipynb)
Now that we have seen how functions can be written for Fugue to bring them to Spark or Dask, we look at how the `transform()` function can be applied with partitioning. In Pandas semantics, this would be the equivalent of a `groupby-apply()`. The difference is partitioning is a core concept in distributed computing as it controls both logical and physical grouping of data.

## [Execution Engine](execution_engine.ipynb)
After seeing how the `transform` function enables the use of Python and Pandas code on Spark, we'll see all of the possible values we can pass as the engine. We can pass strings, cluster addresses, or clients to interact with clusters.

## [Workflows](workflows.ipynb)
This section will cover some concepts like the Directed Acyclic Graph (DAG) and the need for explicit schema in a distributed computing environment. We'll show how to pass parameters to `Transformers`, as well as load and save data. With these, users will be able to start some basic work on data through Fugue.

## [Joining Data](joins.ipynb)
Here we'll show the different ways to join DataFrames in Fugue along with union, intersect, and except. SQL and Pandas also have some inconsistencies that users should be aware of when joining. Fugue maintains consistency with SQL (and Spark).

## [Extensions](beginner_extension.ipynb)
We already covered the `transformer`, the most commonly used Fugue extension. Extensions are Fugue operations on `DataFrames` that are used inside the DAG. Here we will cover the `creator`, `processor`, `cotransformer`, and `outputter`.

## [Distributed Computing](distributed_compute.ipynb)
The heart of Fugue is distributed computing. In this section, we'll show the keywords and concepts that allow Fugue to fully utilize the power of distributed computing. This includes `partitions`, `persisting`, and `broadcasting`.

## [FugueSQL](beginner_sql.ipynb)
We'll show a bit of [FugueSQL](../fugue_sql/index.md), the SQL interface for using Fugue. This is targeted at heavy SQL users and SQL-lovers who want to use SQL on top of Spark and Dask, or even Pandas. FugueSQL is used on DataFrames in memory as opposed to data in databases.

With that, you should be ready to implement data workflows using Fugue.

For full end-to-end examples, check out the [Stock Sentiment](../applications/examples/stock_sentiment.ipynb) and [COVID-19](../applications/examples/example_covid19.ipynb) examples.

For any questions, feel free to join the [Slack channel](http://slack.fugue.ai).
