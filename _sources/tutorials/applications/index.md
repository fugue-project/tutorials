# Applications

This is a list of examples of Fugue applications. Any questions are welcome in the Slack channel.

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ)


```{toctree}
:hidden:

data_validation
databricks_connect
```


## [Data Validation](data_validation.ipynb)
We'll get started with using Fugue and Pandera for data validation. Using Fugue, we can bring Pandas-based libraries into Spark, meaning we don't have to re-implement the same logic twice. Moreover, using Fugue allows us to achieve **validation by partition**, an operation missing in the current data validation frameworks.

## [Fugue and Databricks Connect](databricks_connect.ipynb)
Fugue can be used with the `databricks-connect` library to run code that uses the `SparkExecutionEngine` on a Databricks cluster. Here we'll go over some details.