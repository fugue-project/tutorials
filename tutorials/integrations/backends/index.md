# Backends

Here, we look at the backends supported by Fugue. Backends are the execution engines that Fugue runs code on top of. It is also common to mix and match execution engines. For example, big data processing can happen on SparkSQL and then DuckDB can be used on the smaller processed subset of data.

Have questions? Chat with us on Github or Slack:

[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)

```{toctree}
:hidden:

ibis
dask_sql
duckdb
```

## Python Backends

**This is in addition to Spark, Dask, and Ray**

## [Ibis](ibis.ipynb)
[Ibis](https://github.com/ibis-project/ibis) is a Python framework to write analytical workloads on top of data warehouses (along with DataFrames). Ibis can be used in conjuction with Fugue to query from data warehouses.

## [Polars - UNDER DEVELOPMENT]
[Polars](https://github.com/pola-rs/polars) is a DataFrame library written in Rust (with a Python API) that supports multi-threaded and out-of-core operations. Polars already parallelizes operations well on a local machine. Fugue's integration is focused on allowing Polars code to run on top of a cluster with Spark, Dask, or Ray.

## SQL Backends

**This is in addition to SparkSQL**

## [Dask SQL](dask_sql.ipynb)
[Dask-sql](https://github.com/dask-contrib/dask-sql) is a Dask project that provides a SQL interface on top of Dask DataFrames (including Dask on GPU). FugueSQL can use the Dask-SQL backend to run Dask-SQL and Dask code together.

## [DuckDB](duckdb.ipynb)
[DuckDB](https://duckdb.org/) is an in-process SQL OLAP database management system. It is similar to SQLite but optimized for analytical workloads. DuckDB performs optimizations of queries, allowing it to be 10x - 100x more performant than Pandas in some cases. Good use cases are testing locally, and then moving to SparkSQL when running on big data, or using DuckDB to query initial data before working with local Pandas for more complicated transformations.