# Getting Started

All questions are welcome in the Slack channel.

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ)

Fugue is an abstraction layer that lets users write code in native Python or Pandas, and then port it over to Spark and Dask. This section will cover the motivation of Fugue, the benefits of using an abstraction layer, and how to get started. This section is not a complete reference, but will be sufficient enough to get started with writing full workflows in Fugue.

```{toctree}
:hidden:

introduction
type_flexibility
partitioning
decoupling_logic_and_execution
interface
joins
beginner_extension
distributed_compute
beginner_sql
```


## [Introduction](introduction.ipynb)
We'll get started by introducing Fugue and the simplest way to use it with the `transform()` function. The `transform()` function can take in a Python or pandas function and scale it out in Spark or Dask without having to modify the function. This provides a very simple interface to parallelize Python and pandas code on distributed compute engines.

## [Type Flexbility](type_flexibility.ipynb)
After seeing an example of the `transform()` function, we look into the further flexibility Fugue provides by accepting functions with different input and output types. This allows users to define their logic in whatever expression makes the most sense, and bring native Python functions to Spark or Dask.

## [Partitioning](partitioning.ipynb)
Now that we have seen how functions can be written for Fugue to bring them to Spark or Dask, we look at how the `transform()` function can be applied with partitioning. In pandas semantics, this would be the equivalent of a `groupby-apply()`. The difference is partitioning is a core concept in distrubted compute as it controls both logical and physical grouping of data.

## [Decoupling Logic and Execution](decoupling_logic_and_execution.ipynb)
After seeing how the `transform` function enables the use of Python and pandas code on Spark, we'll see how we can apply this same principle to entire compute workflows using `FugueWorkflow`. We'll show how Fugue allows users to decouple logic from execution, and introduce some of the benefits this provides. We'll go one step further in showing how we use native Python to make our code truly independent of any framework.

## [Fugue Interface](interface.ipynb)
In this section, we'll start covering some concepts like the Directed Acyclic Graph (DAG) and the need for explicit schema in a distributed compute environment. We'll show how to pass parameters to `Transformers`, as well as load and save data. With these, users will be able to start some basic work on data through Fugue.

## [Joining Data](joins.ipynb)
Here we'll show the different ways to join DataFrames in Fugue along with union, intersect, and except. SQL and Pandas also have some inconsistencies users have to be aware of when joining. Fugue maintains consistency with SQL (and Spark).

## [Extensions](beginner_extension.ipynb)
We already covered the `transformer`, the most commonly used Fugue extension. Extensions are Fugue operations on `DataFrames` that are used inside the DAG. Here we will cover the `creator`, `processor`, `cotransformer` and `outputter`.

## [Distributed Compute](distributed_compute.ipynb)
The heart of Fugue is distributed compute. In this section we'll show the keywords and concepts that allow Fugue to fully utilize the power of distributed compute. This includes `partitions`, `persisting`, and `broadcasting`.

## [FugueSQL](beginner_sql.ipynb)
We'll show a bit of [FugueSQL](../fugue_sql/index.md), the SQL interface for using Fugue. This is targeted for heavy SQL users and SQL-lovers who want to use SQL on top of Spark and Dask, or even pandas. FugueSQL that is used on DataFrames in memory as opposed to data in databases.

With that, you should be ready to implement data workflows using Fugue.

For full end-to-end examples, check out the [Stock Sentiment](../examples/stock_sentiment.ipynb) and [COVID-19](../examples/example_covid19.ipynb) examples.

For any questions, feel free to join the [Slack channel](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ).