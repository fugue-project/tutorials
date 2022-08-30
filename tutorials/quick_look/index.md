# Quick Look

The [Fugue](https://github.com/fugue-project/fugue) project aims to make distributed computing effortless. It ports Python, [Pandas](https://pandas.pydata.org/docs/), and SQL code to [Spark](https://spark.apache.org/docs/latest/api/python/), [Dask](https://docs.dask.org/en/stable/), [Ray](https://docs.ray.io/en/latest/index.html), and [DuckDB](https://duckdb.org/docs/). Through Fugue, users only have to worry about defining thier logic in the most intuitive way. Production-ready code can then be scaled out to a distributed computing backend just by adding a few lines of code.

This section contains 10-minute introductions to Fugue and FugueSQL. 

Have questions? Chat with us on Github or Slack:

[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)


```{toctree}
:hidden:

ten_minutes
ten_minutes_sql
```

## [Fugue in 10 Minutes](ten_minutes.ipynb)
Learn the basic Python interface of Fugue by starting with the `transform()` function. This function takes existing Python and Pandas code, and brings it to Spark, Dask, or Ray with minimal re-writes. The `transform()` function alone already allows users to write framework-agnostic code will all its features. It's incrementally adoptable, and users can use it for as little as a single step in their Spark or Dask pipelines.

## [FugueSQL in 10 Minutes](ten_minutes_sql.ipynb)
For users that prefer SQL over Python, Fugue also has a first-class SQL interface to use on top of Pandas, Spark, and Dask DataFrames. FugueSQL is an enhanced version of SQL that has added keywords and syntax intended for end-to-end computing workflows. FugueSQL is parsed, and then ran on the specified backend. For example, FugueSQL using Spark will run on SparkSQL and PySpark.