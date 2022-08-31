# Fugue Tune

Tune is an abstraction layer for general parameter tuning built on top of [Fugue](https://github.com/fugue-project/fugue). It can run hyperparameter tuning frameworks such as optuna and hyperopt on the backends supported by Fugue (Spark, Dask, and Ray). 

Have questions? Chat with us on Github or Slack:

[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)


```{toctree}
:hidden:

search_space
non_iterative
iterative
```

## [Search Space](search_space.ipynb)

Here we learn how to define the search space for hyperparameter tuning. We'll learn how Fugue Tune provides an intuitive and scalable interface for defining hyperparameter combinations for an experiment. The search space is decoupled 

## [Non-iterative Problems](non_iterative.ipynb)

Next we apply the search space on non-iterative problems. These are machine learning models that converge to a solution.

## [Iterative Problems](iterative.ipynb)

Next we apply the search space on iterative problems such as deep learning problems