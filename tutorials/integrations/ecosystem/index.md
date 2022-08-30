# Ecosystem

[Fugue](https://github.com/fugue-project/fugue) can be used in conjuction with a lot of other Python libraries. Some of these integrations are native where Fugue can be used as a backend. For the others, there is no native integration but they can be used together with minimal lines of code, normally through the `transform()` function.

Have questions? Chat with us on Github or Slack:
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)
[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)


```{toctree}
:hidden:

pandera
```

## Data Validation

**[Pandera](pandera.ipynb)**

[Pandera](https://pandera.readthedocs.io/en/stable/) is a lightweight data validation framework originally designed to provide a minimal interface in validating Pandas DataFrames. Pandera has seen expanded to Spark and Dask libraries through Koalas and Modin, but it can also be used pretty seamlessly with Fugue. Fugue also supports validation by partition.

## Machine Learning

**PyCaret (Coming Soon)**

**Nixtla (Coming Soon)**

## Orchestration

**Prefect (Coming Soon)**

**Ploomber (Coming Soon)**