# Fugue Tune

[Tune](https://github.com/fugue-project/tune) is an abstraction layer for general parameter tuning built on top of [Fugue](https://github.com/fugue-project/fugue). It can run hyperparameter tuning frameworks such as [Optuna](https://optuna.org/) and [Hyperopt](http://hyperopt.github.io/hyperopt/) on the backends supported by Fugue (Spark, Dask, Ray, and local). Tune can also be used for general scientific computing in addition to typical machine learning libraries such as [Scikit-learn](https://scikit-learn.org/stable/) and [Keras](https://keras.io/).

Tune has the following goals:

* Provide the simplest and most intuitive APIs for major tuning cases. 
* Be scale agnostic and platform agnostic. We want you to worry less about distributed computing, and just focus on the tuning logic itself. Built on Fugue, Tune let you develop your tuning process iteratively. You can test with small spaces on local machine, and then switch to larger spaces and run distributedly with no code change. 
* Be highly extendable and flexible on lower level abstractions to integrate with libraries such as [Hyperopt](http://hyperopt.github.io/hyperopt/), [Optuna](https://optuna.org/), and [Nevergrad](https://facebookresearch.github.io/nevergrad/).

Have questions? Chat with us on Github or Slack:

[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)

## Installation

Tune is available through pip.

```bash
pip install tune
```

Tune does not come with any machine learning libraries because it can also be used to tune any objective functionn (as in the case of scientific computing). To use it with scikit-learn and [Bayesian Optimization](https://en.wikipedia.org/wiki/Bayesian_optimization), you can install with extras.

```bash
pip install tune[hyperopt,sklearn]
```

```{toctree}
:hidden:

search_space
non_iterative
iterative
```

## Tune Tutorials

### [Search Space](search_space.ipynb)

Here we learn how to define the search space for hyperparameter tuning. We'll learn how Fugue Tune provides an intuitive and scalable interface for defining hyperparameter combinations for an experiment. Tune's search space is decoupled any specific framework.

### [Non-iterative Problems](non_iterative.ipynb)

Next we apply the search space on non-iterative problems. These are machine learning models that converge to a solution. Scikit-learn models fall under this. 

### [Iterative Problems](iterative.ipynb)

Next we apply the search space on iterative problems such as deep learning problems