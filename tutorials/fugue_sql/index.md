# Fugue-sql

`Fugue-sql` is designed for heavy SQL users to extend the boundaries of traditional SQL workflows. `Fugue-sql` allows the expression of logic for end-to-end distributed computing workflows. It can also be combined with Python code to use custom functions alongside the SQL commands. It provides a unified interface, allowing the same SQL code to run on Pandas, Dask, and Spark.

All questions are welcome in the Slack channel.

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ)

The SQL query is parsed with [ANTLR](https://www.antlr.org/) and mapped to the equivalent functions in the `Fugue` programming interface.

```{toctree}
:hidden:

syntax
operators
python
extensions
dask
```

## 1. Installation

In order to use `fugue-sql`, you first need to make sure you have installed the `sql` extra
```
pip install fugue[sql]
```
To run on Spark or Dask execution engines, install the appropriate extras. Alternatively, `all` can be used as an extra.
```
pip install fugue[sql, spark] 
pip install fugue[sql, dask]
pip install fugue[all]
```

## [2. Fugue-sql Syntax](syntax.ipynb)

Get started with `fugue-sql`. This shows input and output of data, enhancements over standard SQL, and how to use SQL to describe computation logic. After this, users will be able to use `fugue-sql` with the familiar SQL keywords to perform operations on top of **Pandas**, **Spark**, and **Dask**.

## [3. Additional SQL Operators](operators.ipynb)

Go over the implemented operations that `Fugue` has on top of the ones provided by standard SQL. `Fugue-sql` is extensible with Python code, but the most common functions are added as built-ins. These include filling NULL values, dropping NULL values, renaming columns, changing schema, etc. This section goes over the most used additional keywords.

## [4. Integrating Python](python.ipynb)

Explore [Jinja templating](https://jinja.palletsprojects.com/) for variable passing, and using a Python functions as a [Transformer](../transformer.ipynb) in a `%%fsql` cell.

## [5. Using Other Fugue Extensions](extensions.ipynb)

The [Transformer](../transformer.ipynb) is just one of many possible [Fugue extensions](../extensions.ipynb). In this section we'll explore the syntax of all the other Fugue extensions: [Creator](../creator.ipynb), [Processor](../processor.ipynb), [Outputter](../outputter.ipynb), and [CoTransformer](../cotransformer.ipynb).

## 6. Fugue-sql with Pandas

`%%fsql` takes in the NativeExecutionEngine as a default parameter. This engine runs on Pandas. All of the SQL operations have equivalents in Pandas, but the behavior can be inconsistent sometimes. For example, Pandas will drop NULL values by default in a groupby operation. The NativeExecutionEngine was designed to mostly make operations consistent with Spark and SQL.

## [7. Fugue-sql with Dask](dask.ipynb)

`Fugue` and [dask-sql](https://dask-sql.readthedocs.io/en/latest/index.html) are collaborating to have our solutions converge and bring the SQL interface for [Dask](https://docs.dask.org/en/latest/). Currently, `dask-sql` is faster on average, while `fugue-sql` is more complete in terms of `SQL` keywords implemented. Conveniently, our solutions can be used together to bring the best of both worlds. This is done by using `dask-sql` as the underlying [execution engine](../execution_engine.ipynb) of the `FugueSQLWorkflow` context manager. 

## 8. Fugue-sql with Spark

`Fugue-sql` also works on **Spark** by passing in the execution engine. This looks like `%%fsql spark`. The operations are mapped to **Spark** and Spark SQL operations. The difference is `fugue-sql` has added functionality for syntax compared to SparkSQL as seen in the [syntax tutorial](syntax.ipynb). Additionally with `fugue-sql`, the same code will execute on Pandas and Dask without modification. This allows for quick testing without having to spin up a cluster. Users prototype with the `NativeExecutionEngine`, and then move to the **Spark** cluster by changing the execution engine.