# Best Practices 

This section is about best practices related to distributed computing, and less about the Fugue framework. One of the things that makes it hard to transition from small data to big data is the mindset. Here, we go over best practices and explain how to fully utilize distributed computing.

Have questions? Chat with us on Github or Slack:

[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)

## [File Format](file_formats.ipynb)
This section explains the difference of CSV files and Parquet files, and why Parquet files are better for big data jobs.

## [Why Fugue is Not Pandas-like](fugue_not_pandas.ipynb)
There are other libraries that promise to distribute Pandas just by changing the import statement. In this section, we explain why Pandas-like frameworks are not meant for distributed computing.

## [Fugue Spark Benchmark](fugue_spark_benchmark.ipynb)
We show that Fugue has a minimal overhead by adding it to the Databricks benchmarks.