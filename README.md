# Welcome To Fugue Tutorials

This environment has everything setup for you, you can run Fugue on native python, Spark and Dask, with Fugue SQL support. In order to setup your own environment, you can pip install the package:

```bash
pip install fugue[all]
```

The simplest way to run the tutorial is to use [mybinder](https://mybinder.org/v2/gh/fugue-project/tutorials/master)

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/fugue-project/tutorials/master)

**BUT please notice that, it runs slow on binder, the spark initialization can take a long time**

To run the tutorials environment on your own machine, the simplest way is, if you have docker installed:

```
docker run -p 8888:8888 fugueproject/tutorials:latest
```

To run docker on your own machine, the performance should be a lot better.

## [For Beginners](tutorials/beginner.ipynb)

## [For Advanced Users](tutorials/advanced.ipynb)