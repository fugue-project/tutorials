---
hide-toc: true
---

# Welcome To Fugue Tutorials

[Join Fugue-Project on Slack](https://join.slack.com/t/fugue-project/shared_invite/zt-ffo2ik1d-maSsCykv_p7kXpnmIjKAug)

This environment has everything setup for you, you can run Fugue on native python, Spark and Dask, with Fugue SQL support. In order to setup your own environment, you can pip install the package:

```bash
pip install fugue[all]
```

The simplest way to run the tutorial is to use [mybinder](https://mybinder.org/v2/gh/fugue-project/tutorials/master)

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/fugue-project/tutorials/master)

**But it runs slow on binder**, the machine on binder isn't powerful enough for
a distributed framework such as Spark. Parallel executions can become sequential, so some of the
performance comparison examples will not give you the correct numbers.

Alternatively, you should get decent performance if running its docker image on your own machine:

```
docker run -p 8888:8888 fugueproject/tutorials:latest
```

# Tutorials

## [For Beginners](tutorials/beginner/index.ipynb)

## [For Advanced Users](tutorials/advanced.ipynb)

## [Fugue-Sql](tutorials/fugue_sql/index.ipynb)

```{toctree}
:hidden:

tutorials/beginner/index
tutorials/advanced
tutorials/fugue_sql/index
tutorials/applications/index
tutorials/resources
```