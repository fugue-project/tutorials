# Backends

Here, we look at the backends supported by Fugue. Backends are the execution engines that Fugue runs code on top of. It is also common to mix and match execution engines. For example, big data processing can happen on SparkSQL and then DuckDB can be used on the smaller processed subset of data.

Have questions? Chat with us on Github or Slack:
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)
[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)

```{toctree}
:hidden:

dask_sql
duckdb
ibis
```

## Python Backends

**This is in addition to Spark, Dask, and Ray**

## [Ibis](ibis.ipynb)
Even if a dataset fits in one core, distributed compute can be used for parallelized model training. We can train multiple models simultaneously. In addition, Fugue provides an easy interface to train multiple models for each logical grouping of data.

## SQL Backends

**This is in addition to SparkSQL**

## [Dask SQL](dask_sql.ipynb)
We'll get started with using Fugue and Pandera for data validation. Using Fugue, we can bring Pandas-based libraries into Spark, meaning we don't have to re-implement the same logic twice. Moreover, using Fugue allows us to achieve **validation by partition**, an operation missing in the current data validation frameworks.

## [Testing Big Data Applications](duckdb.ipynb)
Unit testing is a significant pain point in big data applications. In this section, we examine what makes it so hard to test and how Fugue simplifies it. Through simplified testing, Fugue users often see speedup in the development of big data projects (in addition to lower compute costs).
