# Ecosystem

[Fugue](https://github.com/fugue-project/fugue) can be used in conjuction with a lot of other Python libraries. Some of these integrations are native where Fugue can be used as a backend. For the others, there is no native integration but they can be used together with minimal lines of code, normally through the `transform()` function.

Have questions? Chat with us on Github or Slack:

[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)

```{toctree}
:hidden:

nixtla
pandera
prefect
pycaret
datacompy
```

## Data Validation

**[Pandera](pandera.ipynb)**

[Pandera](https://pandera.readthedocs.io/en/stable/) is a lightweight data validation framework originally designed to provide a minimal interface in validating Pandas DataFrames. Pandera has seen expanded to Spark and Dask libraries through Koalas and Modin, but it can also be used pretty seamlessly with Fugue. Fugue also supports validation by partition.

**[Datacompy](datacompy.ipynb)**

[Datacompy](https://github.com/capitalone/datacompy) is a package to compare two DataFrames of any type. It originally allowed users to compare two Pandas DataFrames or two Spark DataFrames. By adding Fugue as a backend, it can now compare all DataFrames Fugue supports (Pandas, DuckDB, Polars, Arrow, Spark, Dask, Ray, and more).

## Machine Learning

**[PyCaret](pycaret.ipynb)**

[PyCaret](https://pycaret.readthedocs.io/en/stable/) is an open-source low-code machine learning library that allows users to train dozens of models in a few lines of code. With a native integration, Fugue users and distribute the machine learning training over Spark, Dask or Ray.

**[Nixtla](nixtla.ipynb)**

[Nixtla](https://github.com/Nixtla/nixtla) is a project focused on state-of-the-art time series modelling. The current Fugue integration is around their statistical forecasting packages named [statsforecast](https://github.com/Nixtla/statsforecast). Fugue lets users apply `AutoARIMA` and `ETS` models to forecast millions of independent timeseries on top of distributed compute.

## Orchestration

**[Prefect](prefect.ipynb)**

[Prefect] is an open-source workflow orchestration framework used for scheduling and monitoring tasks. The `prefect-fugue` collection allows users to iterate locally, and then bring the code to Databricks or Coiled for execution when production ready.

**Ploomber (Coming Soon)**