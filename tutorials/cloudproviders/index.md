# Fugue with Cloud Providers

All questions are welcome in the Slack channel.

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)

This section will cover using Fugue on top of cloud providers such as Databricks or Coiled. Fugue has a `fugue-cloudproviders` package that allows users to easily spin up ephemeral compute for their compute workflows.

```{toctree}
:hidden:

databricks
coiled
```

## [Databricks](databricks.ipynb)

Databricks is the most common provider for Spark clusters. Using the `databricks-connect` library, we can easily spin up an ephemeral Spark cluster. We can connect to the SparkSession on the Databricks cluster from a local machine.

## [Coiled](coiled.ipynb)

[Coiled](https://coiled.io/) is the easiest way to host Dask clusters on the cloud. Using the [coiled](https://pypi.org/project/coiled/) library, we can easily spin up an ephemeral Dask cluster or connect to an existing Dask cluster on Coiled.