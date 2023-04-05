---
hide-toc: true
---

# Welcome to the Fugue Tutorials!

Have questions? Chat with us on Github or Slack:

[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)


[Fugue](https://github.com/fugue-project/fugue) provides an easier interface to using distributed compute effectively and accelerates big data projects. It does this by minimizing the amount of code you need to write, in addition to taking care of tricks and optimizations that lead to more efficient execution on distrubted compute. Fugue ports Python, Pandas, and SQL code to Spark, Dask, and Ray.

![img](images/fugue_backends.png)

Quick Links:

* Scaling Pandas code to Spark, Dask, or Ray? Start with [Fugue in 10 minutes](tutorials/quick_look/ten_minutes.ipynb).
* Need a SQL interface on top of Pandas, Spark and Dask? Check [FugueSQL in 10 minutes](tutorials/quick_look/ten_minutes_sql.ipynb).
* For previous conference presentations and blog posts, check the [Content page](tutorials/resources/content.md).

## Installation

In order to setup your own environment, you can pip (or conda) install the package. Fugue can then

```bash
pip install fugue
```

Backend engines are installed separately through pip extras. For example, to install Spark:

```bash
pip install fugue[spark]
```

If Spark, Dask, or Ray are already installed on your machine, Fugue will be able to detect it. Spark requires Java to be installed separately.

## Running the Code

The simplest way to run the tutorial interactively is to use [mybinder](https://mybinder.org/v2/gh/fugue-project/tutorials/master). Binder spins up an environment using a container.

>- **Some code snippets run slow on binder** as the machine on binder isn't powerful enough for a distributed framework such as Spark.
>- Parallel executions can become sequential, so some of the performance comparison examples will not give you the correct numbers.

Alternatively, you should get decent performance if running the Docker image on your own machine:

```
docker run -p 8888:8888 fugueproject/tutorials:latest
```

```{toctree}
:maxdepth: 6
:caption: Quick Look
:hidden:

tutorials/quick_look/ten_minutes
tutorials/quick_look/ten_minutes_sql
```

```{toctree}
:maxdepth: 6
:caption: Tutorials
:hidden:

tutorials/beginner/index
tutorials/advanced/index
tutorials/fugue_sql/index
tutorials/extensions/index
```

```{toctree}
:maxdepth: 6
:caption: Integrations
:hidden:

tutorials/integrations/backends/index
tutorials/integrations/cloudproviders/index
tutorials/integrations/warehouses/index
tutorials/integrations/ecosystem/index
```

```{toctree}
:caption: Applications
:hidden:

tutorials/applications/use_cases/index
tutorials/applications/examples/index
tutorials/applications/recipes/index
tutorials/applications/debugging/index
```

```{toctree}
:caption: Fugue Libraries
:hidden:

tutorials/tune/index
```


```{toctree}
:caption: Resources
:hidden:

tutorials/resources/appendix/index
tutorials/resources/best_practices/index
tutorials/resources/content
tutorials/resources/major_changes
```
