---
hide-toc: true
---

# Welcome to the Fugue Tutorials!

All questions are welcome in the Slack channel.

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/fugue-project/tutorials/master) ⬅️  Launch these tutorials in Binder

[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue) ⬅️  Check out our source code

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ) ⬅️  Chat with us on Slack

## What Does Fugue Do?

[Fugue](https://github.com/fugue-project/fugue) provides an easier interface to using distributed compute effectively and accelerates big data projects. It does this by minimizing the amount of code you need to write, in addition to taking care of tricks and optimizations that lead to more efficient execution on distrubted compute.

Quick Links:
* Bringing a Python/Pandas function to Spark or Dask? Check the [Fugue Transform](introduction.html#fugue-transform) section.
* Need a SQL interface on top of Pandas, Spark and Dask? Check the [FugueSQL](tutorials/fugue_sql/index.md) section.
* For previous conference presentations and blog posts, Check the [Resources](tutorials/resources.md) section.

## Installation

In order to setup your own environment, you can pip install the package. This includes Fugue on native python, Spark and Dask, with Fugue SQL support.

>- Spark requires Java to be installed separately.

```bash
pip install fugue[all]
```

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

DuckDB <tutorials/integrations/duckdb.ipynb>
Ibis <tutorials/integrations/ibis.ipynb>
Dask-sql <tutorials/integrations/dasksql.ipynb>
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
:caption: Further Information
:hidden:


tutorials/resources
tutorials/appendix/index
```
