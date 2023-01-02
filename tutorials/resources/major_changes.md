# Major Changes

This is a record of major changes for existing users to get a quick summary of new features released with each version. It is not meant to be a comprehensive changelog.

Have questions? Chat with us on Github or Slack:

[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)

## 0.8.0

Fugue 0.8.0 has significant changes to the standard ways to use Fugue. Having users write code with the `FugueWorkflow` had some painpoints including:

* a lot of boilerplate code when wrapping engine-agnostic functions
* all custom code had to fit extensions, increasing the concepts users needed to learn
* FugueWorkflow was needed to use any engine-agnostic functions Fugue provided like `load()`, `save()` and `fillna()`
* difficulty in getting the schema after an operation

As such, Fugue is moving away from the `FugueWorkflow()` (DAG) model and allowing users to use as litle of Fugue as they want. The result is a collection of standalone engine-agnostic functions that users can invoke in their code. The DAG model gave some benefits in terms because it allowed for compilation time checks before code was executed on a cluster. Users could immediately be notified is there was a schema mismatch. Another feature that was enabled by the DAG was the auto-persist feature to prevent recomputation of DataFrames used mutliple times.

Ultimately, the code cost outweighed the benefits for a lot of users. There were several occasions were users had to adjust too much code to bring it to `FugueWorkflow()`. The heaviness of the API was less natural for users bringing existing code to distributed computing.

As such, two complementary pieces are now available in place of `FugueWorkflow()`. These are the Fugue API and the `engine_context()`. Tutorials for them have been added in the [Getting Started](../beginner/index.md) section, but we will also show them here.

### Fugue API

For the list of functions, see [Transformations](../beginner/transformations.ipynb)

Previously, Fugue had two programmatic ways of being used with `transform()` and `FugueWorkflow()`. 
The `transform()` function proved to be easy to use for users bringing one function to distributed 
computing. The problem was the overhead to transition to `FugueWorkflow()` for end-to-end data 
workflows. In a lot of cases, Fugue users just wanted to use one function of the Fugue API. To 
simplify the experience, the Fugue API now offers all of its functions as standalone functions 
that are compatible with any execution engine.

`FugueWorkflow()` is still currently supported. This is not a breaking change, and FugueSQL users
still use `FugueWorkflow()` under the hood.s

Before:

```python
with FugueWorkflow("spark") as dag:
    df = dag.load("path/to/file")
    df = df.transform(df, fn, ...)
    dag.save(df)
```

Current:

```python
import fugue.api as fa

df = fa.load("path/to/file", engine="spark")
df = fa.transform(df, fn, ..., engine="spark")
fa.save(df, engine="spark")
```

Note that all of the functions here can be used independently with any of the execution engines 
that Fugue supports. The full list of functions can be found in the 
[top-level reference](https://fugue.readthedocs.io/en/latest/top_api.html#transformation) and some
examples can be found in the [Transformations section](../beginner/transformations.ipynb).

### Engine Context

In the code snippet above, it can be redundant to type out `engine="spark"` multiple times. In this
case, we can just use the `engine_context()` to set a default engine for all API operations under.

```python
import fugue.api as fa

with fa.engine("spark"):
    df = fa.load("path/to/file")
    df = fa.transform(df, fn, ...)
    fa.save(df)
```

and all of these operations will run under Spark. For more examples, see the 
[Engine Context section](../beginner/engine_context.ipynb).

### transform() engine inference

Another quality of life enhancement is that the `transform()` function can
now infer the engine to use based on the DataFrame passed in.

```python
transform(df, fn, ...)          # runs on Pandas
transform(spark_df, fn, ...)    # runs on Spark
transform(dask_df, fn, ...)     # runs on Dask
transform(ray_df, fn, ...)      # runs on Ray
```

## 0.7.1

### File I/O with `transform()`

While the `transform()` function achieved running functions across any execution engine, a common
problem was that users still had to write code to load and save files. To do this in an engine-agnostic
way, they had to use `FugueWorkflow()` as follows:

```python
with FugueWorkflow("spark") as dag:
    df = dag.load("path/to/file")
    df = df.transform(df, fn, ...)
    dag.save(df)
```

This increased the amount of boilerplate code just to read and write files. Thus, the `transform()`
function was enhanced to support file input and output.

```python
transform("/tmp/f.parquet", fn, schema="*", engine="dask", save_path="/tmp/f_out.parquet")
```

There are two main changes reflected here:

1. The input can now be a file path and Fugue will load it in using the appropriate engine
2. The save path can be used to save the output. If supplied, it will be saved using the 
execution engine. It will return the file path, allowing users to chain together succeeding
`transform()` calls.

More information can be found in the [loading and saving](../beginner/io.ipynb) section of
the tutorials.
