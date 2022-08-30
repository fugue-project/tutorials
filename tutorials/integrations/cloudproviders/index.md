# Cloud Providers

Since [Fugue](https://github.com/fugue-project/fugue) is a framework for distributed compute, it is often paired with a solution that manages Spark, Dask, or Ray clusters. This section will cover using Fugue on top of cloud providers such as Databricks or Coiled. Fugue has a [`fugue-cloudprovider`](https://github.com/fugue-project/fugue-cloudprovider) package that allows users to easily spin up ephemeral compute for their compute workflows.

Have questions? Chat with us on Github or Slack:
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)
[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)


```{toctree}
:hidden:

databricks
coiled
```

## Spark

**[Databricks](databricks.ipynb)**

[Databricks](https://www.databricks.com/) is the most common provider for Spark clusters. Using the `databricks-connect` library, we can easily spin up an ephemeral Spark cluster. We can connect to the SparkSession on the Databricks cluster from a local machine.

## Dask

**[Coiled](coiled.ipynb)**

[Coiled](https://coiled.io/) is the easiest way to host Dask clusters on the cloud. Using the [coiled](https://pypi.org/project/coiled/) library, we can easily spin up an ephemeral Dask cluster or connect to an existing Dask cluster on Coiled.