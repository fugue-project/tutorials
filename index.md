---
hide-toc: true
---

# Welcome to the Fugue Tutorials!

Have questions? Chat with us on Github or Slack:
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)
[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)

## What Does Fugue Do?

[Fugue](https://github.com/fugue-project/fugue) provides an easier interface to using distributed compute effectively and accelerates big data projects. It does this by minimizing the amount of code you need to write, in addition to taking care of tricks and optimizations that lead to more efficient execution on distrubted compute.

Quick Links:

* Scaling Pandas code to Spark, Dask, or Ray? Start with [Fugue in 10 minutes](tutorials/quick_look/ten_minutes.ipynb)
* Need a SQL interface on top of Pandas, Spark and Dask? Check the [FugueSQL in 10 minutes](tutorials/quick_look/ten_minutes_sql.ipynb) section.
* For previous conference presentations and blog posts, Check the [Resources](tutorials/resources.md) section.

## Installation

In order to setup your own environment, you can pip (or conda) install the package. This includes Fugue on native python, Spark and Dask, with Fugue SQL support.

>- Spark requires Java to be installed separately.

```bash
pip install fugue
```

Backend engines are installed separately through pip extras. For example, to install Spark:

```bash
pip install fugue[spark]
```

If Spark, Dask, or Ray are already installed on your machine, Fugue will be able to detect it. 

## Running the Code

The simplest way to run the tutorial interactively is to use [mybinder](https://mybinder.org/v2/gh/fugue-project/tutorials/master). Binder spins up an environment using a container.

>- **Some code snippets run slow on binder** as the machine on binder isn't powerful enough for a distributed framework such as Spark.
>- Parallel executions can become sequential, so some of the performance comparison examples will not give you the correct numbers.

Alternatively, you should get decent performance if running its docker image on your own machine:

```
docker run -p 8888:8888 fugueproject/tutorials:latest
```

```{toctree}
:maxdepth: 6
:caption: Quick Look
:hidden:


tutorials/quick_look/index
```

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
:maxdepth: 6
:caption: Integrations
:hidden:

tutorials/integrations/backends/index
tutorials/integrations/cloudproviders/index
tutorials/integrations/ecosystem/index
```

```{toctree}
:caption: Applications
:hidden:

tutorials/examples/index
tutorials/recipes/index
tutorials/applications/index
tutorials/debugging/index
```

```{toctree}
:caption: Fugue Tune
:hidden:

tutorials/tune/index
```


```{toctree}
:caption: Resources
:hidden:

tutorials/resources/appendix/index
tutorials/resources/best_practices/index
tutorials/resources/content
```
