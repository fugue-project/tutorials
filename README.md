# <img src="./images/logo_blue.svg" width="200">

[![PyPI version](https://badge.fury.io/py/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![PyPI pyversions](https://img.shields.io/pypi/pyversions/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![PyPI license](https://img.shields.io/pypi/l/fugue.svg)](https://pypi.python.org/pypi/fugue/)
[![codecov](https://codecov.io/gh/fugue-project/fugue/branch/master/graph/badge.svg?token=ZO9YD5N3IA)](https://codecov.io/gh/fugue-project/fugue)
[![Codacy Badge](https://app.codacy.com/project/badge/Grade/4fa5f2f53e6f48aaa1218a89f4808b91)](https://www.codacy.com/gh/fugue-project/fugue/dashboard?utm_source=github.com&utm_medium=referral&utm_content=fugue-project/fugue&utm_campaign=Badge_Grade)
[![Downloads](https://pepy.tech/badge/fugue)](https://pepy.tech/project/fugue)

| Tutorials | API Documentation | Chat with us on slack! |
| --- | --- | --- |
| [![Jupyter Book Badge](https://jupyterbook.org/badge.svg)](https://fugue-tutorials.readthedocs.io/) | [![Doc](https://readthedocs.org/projects/fugue/badge)](https://fugue.readthedocs.org)  | [![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai) |

**Fugue is a unified interface for distributed computing that lets users execute Python, pandas, and SQL code on Spark, Dask, and Ray with minimal rewrites**.

## [Tutorials](https://fugue-tutorials.readthedocs.io/)

The best way to get started with Fugue is to work through the 10 minute tutorials:

*   [Fugue in 10 minutes](https://fugue-tutorials.readthedocs.io/tutorials/quick_look/ten_minutes.html)
*   [FugueSQL in 10 minutes](https://fugue-tutorials.readthedocs.io/tutorials/quick_look/ten_minutes_sql.html)

## Running Tutorials Interactively

### Using Binder

[![Binder](https://mybinder.org/badge_logo.svg)](https://mybinder.org/v2/gh/fugue-project/tutorials/master)

**Note it runs slow on binder** because the machine on binder isn't powerful enough for a distributed framework such as Spark. Parallel executions can become sequential, so some of the performance comparison examples will not give you the correct numbers.

### Using Docker

Alternatively, you should get decent performance by running this Docker image on your own machine:

```bash
docker run -p 8888:8888 fugueproject/tutorials:latest
```

## Community

Feel free to message us on [Slack](http://slack.fugue.ai)