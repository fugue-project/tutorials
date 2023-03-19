# Major Changes

This is a record of major changes for existing users to get a quick summary of new features released with each version. It is not meant to be a comprehensive changelog.

Have questions? Chat with us on Github or Slack:

[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)

## 0.8.1

Fugue 0.8.1 has changes to enhance FugueSQL further.

* Not directly tied to FugueSQL, there is a new repo and library [fugue-warehouses](https://github.com/fugue-project/fugue-warehouses) that contains code for using FugueSQL syntax on top of data warehouses. The first release is FugueSQL on top of BigQuery.
* The extension model has been brought to FugueSQL has well. This allows users to invoke Python libraries on top of SQL DataFrames with one line of code. For people interested in contributing, this is a good place to start. As an example, see the [seaborn integration](https://github.com/fugue-project/fugue/blob/master/fugue_contrib/seaborn/__init__.py).

### Fugue BigQuery

The roadmap of the Fugue project includes supporting data warehouses more. BigQuery is the first one to be released. The full documentation can be found under the [warehouses section](../integrations/warehouses/) of the tutorials. With this, Fugue users can use the FugueSQL syntax on top of BigQuery tables. 

FugueSQL improves the developer experience by minimizing the boilerplate code that users have to write. It also helps in breaking up the logic so that users can iterate and test their SQL queries more quickly.

For example in a Jupyter notebook cell:

```
%%fsql bq
SELECT col1, SUM(col2) AS col2
  FROM df
 GROUP BY col1

TAKE 5 ROWS PRESORT col2 DESC
YIELD DATAFRAME
```

For more information about the syntax, check the [10 mins to FugueSQL](../quick_look/ten_minutes_sql.ipynb)

If Python transformations are invoked using `TRANSFORM` or `OUTPUT`, then the data can be brought down to Pandas or a Python distributed backend like Spark, Dask, or Ray. It is suggested to try to pre-aggregate as much as possible on the SQL table to minimize data transfer.


### SQL Extensions

The introduction of extensions allows users to invoke Python code on any backend. This can be particularly useful and working on warehouses. For example:

```sql
SELECT name, year, SUM(number) AS ct
  FROM `table_name`
 GROUP BY 1, 2
 ORDER BY 2
 OUTPUT USING sns:lineplot(x="year",y="ct",hue="name")
```

would give a seaborn lineplot. The list of extensions can be found in [fugue-contrib](https://github.com/fugue-project/fugue/tree/master/fugue_contrib) but they will be added to the documentation in the future as well.

## 0.8.0

Fugue 0.8.0 has significant changes to the standard ways to use Fugue. Having users write code with the `FugueWorkflow` had some painpoints including:

* a lot of boilerplate code when wrapping engine-agnostic functions
* all custom code had to fit extensions, increasing the concepts users needed to learn
* FugueWorkflow was needed to use any engine-agnostic functions Fugue provided like `load()`, `save()` and `fillna()`
* difficulty in getting the schema after an operation

As such, Fugue is moving away from the `FugueWorkflow()` (DAG) model and allowing users to use as litle of Fugue as they want. The result is a collection of standalone engine-agnostic functions that users can invoke in their code. The DAG model gave some benefits in terms because it allowed for compilation time checks before code was executed on a cluster. Users could immediately be notified is there was a schema mismatch. Another feature that was enabled by the DAG was the auto-persist feature to prevent recomputation of DataFrames used mutliple times.

Ultimately, the code cost outweighed the benefits for a lot of users. There were several occasions were users had to adjust too much code to bring it to `FugueWorkflow()`. The heaviness of the API was less natural for users bringing existing code to distributed computing.

As such, two complementary pieces are now available in place of `FugueWorkflow()`. These are the Fugue API and the `engine_context()`. Tutorials for them have been added in the [Getting Started](../beginner/index.md) section, but we will also show them here.

There are also changes regarding FugueSQL with two new exposed functions in `fugue_sql()` and `fugue_sql_flow()`. `fugue_sql_flow()` matches the previous `fsql`, including the `.run()` method while the new `fugue_sql()` will return the latest DataFrame in a query automatically. There is also `fugue.api.raw_sql`, which matches the behavior of the previous `FugueWorkflow().df(...).select()`. These changes are to cater to SQL users who expected `fsql()` to return a DataFrame. The new setup will be more convenient.

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

**All joins are also available as standalone functions now**

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

### FugueSQL Changes

Users that are new to Fugue often expect that `fsql()` automatically returns a DataFrame. Because the
expectation of SQL users is the `SELECT` returns something, a `fugue_sql()` function will now return
the last DataFrame. For example:

```python
from fugue.api import fugue_sql 

result = fugue_sql("""
LOAD "/tmp/df.parquet"

SELECT col1, MAX(col2) AS max_val
 GROUP BY col1
""", engine=None)

result.head()
```

It can also return a DataFrame associated with the engine. For example,
using Spark as the engine will return a Spark DataFrame.

The previous `fsql()` is now renamed to `fugue_sql_flow()` and behaves the same.

```python
from fugue.api import fugue_sql_flow

fugue_sql_flow("""
LOAD "/tmp/df.parquet"

SELECT col1, MAX(col2) AS max_val
 GROUP BY col1
 PRINT
""").run(engine="spark");
```

### Special Column Names

Fugue previously did not support column names with spaces. The ` character can now be used to specify
a column name with spaces. This will work across all backends. The schema expression will look like:

````    
`a b`:int,b:str
````

or to escape, use the double backtick:

````
`a``b`:int,b:str
````

where the column names will be a`b and b.

### Raw SQL

`FugueWorkflow` DataFrames had a method `.select()` that could be used as follows:

```python
from fugue import FugueWorkflow

with FugueWorkflow() as dag:
    df = dag.load("../../data/stock_sentiment_data.csv", header=True)
    df.show()
    dag.select("Sentiment, COUNT(*) AS ct FROM",df, "GROUP BY Sentiment").show()
```

This has now been changed to `fugue.api.raw_sql`

```python
import fugue.api as fa

df = fa.load("../../data/stock_sentiment_data.csv", header=True)
fa.raw_sql("Sentiment, COUNT(*) AS ct FROM",df, "GROUP BY Sentiment")
```

### Utility Functions

The Fugue API now has utility functions that can be used to get information of the DataFrame.

* `fa.get_schema()`
* `fa.get_column_names()`
* `fa.is_local()`
* `fa.peek_dict()` - returns first row as dict
* `fa.peek_array()` - returns first row as array

The full list can be found in the [top-level API Information](https://fugue.readthedocs.io/en/latest/top_api.html#information) section. 

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
