# Fugue Tune

Tune is an abstraction layer for general parameter tuning built on top of [Fugue](https://github.com/fugue-project/fugue). It can run on the backends supported by Fugue (Spark and Dask). All questions are welcome in the Slack channel.

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ)


```{toctree}
:hidden:

search_space
non-iterative
```

## [Search Space](search_space.ipynb)

Here we learn how to define the search space for hyperparameter tuning. We'll learn how Fugue Tune provides an intuitive and scalable interface for defining combinations for hyperparameters for an experiment.

## [Non-iterative Problems](non-iterative.ipynb)

Next we apply the search space on non-iterative problems. These are machine learning models that converge to a solution.