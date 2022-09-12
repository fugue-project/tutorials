# Use Cases

This is a list of examples of Fugue use cases. Any questions are welcome in the Slack channel.

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)
[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)

```{toctree}
:hidden:

unit_testing
model_sweeping
databricks_connect
nlp
```

## [Testing Big Data Applications](unit_testing.ipynb)
Unit testing is a significant pain point in big data applications. In this section, we examine what makes it so hard to test and how Fugue simplifies it. Through simplified testing, Fugue users often see speedup in the development of big data projects (in addition to lower compute costs).

## [Data Validation](../../integrations/ecosystem/pandera.ipynb)
We'll get started with using Fugue and Pandera for data validation. Using Fugue, we can bring Pandas-based libraries into Spark, meaning we don't have to re-implement the same logic twice. Moreover, using Fugue allows us to achieve **validation by partition**, an operation missing in the current data validation frameworks.

## [Distributed Model Sweeping](model_sweeping.ipynb)
Even if a dataset fits in one core, distributed compute can be used for parallelized model training. We can train multiple models simultaneously. In addition, Fugue provides an easy interface to train multiple models for each logical grouping of data.

## [Natural Lanugage Processing](nlp.ipynb)
Fugue helps parallelize Natural Language Processing (NLP), especially during pre-processing steps which tend to be executed for each row of data. This page shows an example of how to use the `transform()` function to do some pre-processing.


## Using Fugue with Providers

This section is how to connect without using the native Fugue integrations. Fugue has integrations with some cloud providers in the `fugue-cloudprovider` repo. For more native integrations to spin-up ephemeral clusters, check [Fugue with Cloud Providers](../integrations/cloudproviders/index.md)

### [Databricks Connect](databricks_connect.ipynb)
Fugue can be used with the `databricks-connect` library to run code on a Databricks cluster by using the SparkSession. Here we'll go over some details of how to set it up.
