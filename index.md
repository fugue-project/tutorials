---
hide-toc: true
---

# Welcome To Fugue Tutorials

All questions are welcome in the Slack channel.

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ)

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

## [Getting Started](tutorials/beginner/index.md)

## [Extensions](tutorials/extensions/index.md)

## [Fugue-Sql](tutorials/fugue_sql/index.md)

## [Deep Dive](tutorials/advanced/index.md)

## [Examples](tutorials/examples/index.md)

## [Applications](tutorials/applications/index.md)

## [Resources](tutorials/resources.md)

## [Appendix](tutorials/appendix/index.md)

```{toctree}
:maxdepth: 6
:caption: Tutorials
:hidden:


tutorials/beginner/index
tutorials/extensions/index
tutorials/fugue_sql/index
tutorials/advanced/index
```

```{toctree}
:caption: Applications
:hidden:


tutorials/examples/index
tutorials/applications/index
```

```{toctree}
:caption: Further Information
:hidden:


tutorials/resources
tutorials/appendix/index
```