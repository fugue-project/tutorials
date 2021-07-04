# Getting Started

Welcome to the Fugue tutorials. All questions are welcome in the Slack channel.

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ)

Fugue is an abstraction framework that lets users write code in native Python or Pandas, and then port it over to Spark and Dask. The beginner tutorial will cover the motivation of Fugue, the benefits of using an abstraction layer, and how to get started. The beginner section is not a complete reference, but sufficient enough to get started with writing full workflows in Fugue.

```{toctree}
:hidden:

introduction
interface
joins
beginner_extension
distributed_compute
beginner_sql
```


## [1. Introduction](introduction.ipynb)
We'll start by going over the motivation for Fugue and the problems it solves. We'll show some basic Fugue code, and how to execute code on different ExecutionEngines (Spark or Dask). Fugue is more than a framework. It is a mindset for working on data problems, especially ones involving distributed compute. In this section we'll cover some of the beliefs of Fugue.


## [2. Fugue Interface](interface.ipynb)
In this section we'll start covering some concepts like the Directed Acyclic Graph (DAG) and the need for explicit schema in a distributed compute environment. We'll show how to pass parameters to `transformers`, as well as load and save data. With these, users will be able to start some basic work on data through Fugue.


## [3. Joining Data](joins.ipynb)
Here we'll show the different ways to join DataFrames in Fugue along with union, intersect, and except. SQL and Pandas also have some inconsistencies users have to be aware of when joining. Fugue maintains consistency with SQL (and Spark).


## [4. Extensions](beginner_extension.ipynb)
We already covered the `transformer`, the most commonly used Fugue extension. Extensions are Fugue operations on DataFrames that are used inside the DAG. Here we will cover the `creator`, `processor`, `cotransformer` and `outputter`.


## [5. Distributed Compute](distributed_compute.ipynb)
The heart of Fugue is distributed compute. In this section we'll show the keywords and concepts that allow Fugue to fully utilize the power of distributed compute. This includes `partitions`, `persisting`, and `broadcasting`.


## [6. Fugue-SQL](beginner_sql.ipynb)
As a last note, we'll show a bit on Fugue-Sql, the SQL interface for using Fugue. This is targeted for heavy SQL users and SQL-lovers who want to use SQL on top of Spark and Dask, or even Pandas. This is SQL that is used on DataFrames in memory as opposed to data in databases.

With that, you should be ready to implement data workflows using Fugue. For full end-to-end examples, check out the [Stock Sentiment](../stock_sentiment.ipynb) and [COVID-19](../example_covid19.ipynb) examples. For any questions, feel free to join the [Slack channel](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ).