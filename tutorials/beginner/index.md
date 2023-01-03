# Getting Started

Have questions? Chat with us on Github or Slack:

[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)

Fugue is an abstraction layer that lets users write code in native Python or Pandas and then port it over to Spark, Dask, and Ray. This section will cover the motivation of Fugue, the benefits of using an abstraction layer, and how to get started. This section is not a complete reference but will be sufficient to get started with writing full workflows in Fugue.


```{toctree}
:hidden:

transform
type_hinting
schema
partitioning
execution_engine
io
engine_context
joins
transformations
distributed_compute
beginner_sql
```

## [transform() Function](transform.ipynb)
We'll get started by introducing Fugue and show some motivating uses cases. The `transform()` function can take in a Python or Pandas function and scale it out in Spark or Dask without having to modify it. This provides a very simple interface to parallelize Python and Pandas code on distributed computing engines.

## [Type Hinting](type_hinting.ipynb)
After diving into the `transform()` function, we look into the further flexibility Fugue provides by accepting functions with different input and output types. This allows users to define their logic in whatever expression makes the most sense and bring native Python functions to Spark, Dask or Ray. Having flexibility is important because distributed computing often goes beyond the scope of processing Pandas-like DataFrames. Think of aggregating API calls or processing image data.

## [Schema](schema.ipynb)
Schema is an important part of distributed computing. Some frameworks even require it because schema inference can be especially expensive or inaccurate. Fugue has it's own schema implementation that is a simplified in syntax. This section will look into Fugue's schema expression.

## [Partitioning](partitioning.ipynb)
Now that we have seen how functions can be written for Fugue to bring them to Spark or Dask, we look at how the `transform()` function can be applied with partitioning. In Pandas semantics, this would be the equivalent of a `groupby-apply()`. The difference is partitioning is a core concept in distributed computing as it controls both logical and physical grouping of data.

## [Execution Engine](execution_engine.ipynb)
After seeing how the `transform` function enables the use of Python and Pandas code on Spark, we'll see all of the possible values we can pass as the engine. We can pass strings, cluster addresses, or clients to interact with clusters.

## [Saving and Loading](io.ipynb)
Similar to the `transform()` function, the Fugue API also has saving and loading functions compatible with Pandas, Spark, Dask, and Ray. These help in constructing end-to-end workflows that can then be ran on top of any backend.

## [Engine Context](engine_context.ipynb)
Often, we will have multiple operations that use the same execution engine. Instead of having to pass in the engine each time, we can use the `engine_context()` of the Fugue API. This will set the default execution engine for all Fugue API function calls.

## [Joins](joins.ipynb)
Here we'll show the different ways to join DataFrames in Fugue along with union, intersect, and except. SQL and Pandas also have some inconsistencies that users should be aware of when joining. Fugue maintains consistency with SQL (and Spark).

## [Transformations](transformations.ipynb)
Here we'll show the different ways to join DataFrames in Fugue along with union, intersect, and except. SQL and Pandas also have some inconsistencies that users should be aware of when joining. Fugue maintains consistency with SQL (and Spark).

## [Distributed Computing](distributed_compute.ipynb)
The heart of Fugue is distributed computing. In this section, we'll show the keywords and concepts that allow Fugue to fully utilize the power of distributed computing. This includes `partitions`, `persisting`, and `broadcasting`.

## [FugueSQL](beginner_sql.ipynb)
We'll show a bit of [FugueSQL](../fugue_sql/index.md), the SQL interface for using Fugue. This is targeted at heavy SQL users and SQL-lovers who want to use SQL on top of Spark and Dask, or even Pandas. FugueSQL is used on DataFrames in memory as opposed to data in databases.

With that, you should be ready to implement data workflows using Fugue.

For full end-to-end examples, check out the [Stock Sentiment](../applications/examples/stock_sentiment.ipynb) and [COVID-19](../applications/examples/example_covid19.ipynb) examples.

For any questions, feel free to join the [Slack channel](http://slack.fugue.ai).
