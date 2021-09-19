---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.10.3
kernelspec:
  display_name: 'Python 3.7.9 64-bit (''fugue-tutorials'': conda)'
  metadata:
    interpreter:
      hash: 131b24c7e1bb8763ab2b04d5b6d98a68c7b3a823a2a57c5722935f7690890f70
  name: python3
---

# Syntax


The `FugueSQL` syntax is between standard SQL, JSON, and Python. The goals are

* To be fully compatible with standard `SQL SELECT` statement
* To minimize syntax overhead, to make code as short as possible while still easy to read
* Allow users to fully describe their compute logic in SQL as opposed to Python

To achieve these goals, enhancements were made to the standard SQL syntax that will be demonstrated here.

+++

## Hello World

First, we start with the basic syntax `FugueSQL`. We import `fugue_notebook`, which contains a Jupyter notebook extension. Fugue has both a Python interface, and SQL interface which have equivalent functionality. 

The `setup` function in the cell below provides syntax highlighting for `FugueSQL` users. At the moment, syntax highlighting will not work for JupyterLab notebooks.

```{code-cell} ipython3
from fugue_notebook import setup
setup()
```

```{code-cell} ipython3
%%fsql
CREATE [[0,"hello"],[1,"world"]] SCHEMA number:int,word:str
PRINT
```

The `CREATE` keyword here is a `FugueSQL` keyword. We'll dive into [extensions](./extensions.ipynb) later and learn more about integrating Python functions into FugueSQL.

+++

## SQL Compliant

All standard SQL keywords are available in `FugueSQL`. In this example, `GROUP BY`, `WHERE`, `SELECT`, `FROM` are all the same as standard SQL.

```{code-cell} ipython3
# Defining data
import pandas as pd
data = pd.DataFrame({"id": ["A","A","A","B","B","B"],
                    "date": ["2020-01-01", "2020-01-02",
                             "2020-01-03", "2020-01-01", 
                             "2020-01-02", "2020-01-03"],
                    "value": [10, None, 30, 20, None, 40]})
```

```{code-cell} ipython3
%%fsql
SELECT id, date, MIN(value) value
FROM data
WHERE value > 20
GROUP BY id
PRINT
```

Note that the Pandas DataFrame `df` was accessed inside the SQL expression. DataFrames defined in Python cells are automatically accessible by SQL cells. Other variables need to be passed in through [Jinja templating](./syntax.ipynb). More on this will be shown when we explore how Python and fugue-sql interact.

The example above shows the possibility of combining Python and SQL workflows. This is useful if Python needs to connect to other place (AWS S3, Azure Blob Storage, Google Analytics) to retrieve data that is needed for the compute workflow. The data can be loaded in with Python and passed to `%%fsql` cells. 

+++

## Input and Output

Actual data work often require loading in the `DataFrame`. `Fugue` has two keywords in `SAVE` and `LOAD`. Using these allow `FugueSQL` users to orchestrate their ETL jobs with SQL logic. A csv file can be loaded in, transformed, and then saved elsewhere. Full data analysis and transformation workflows can be done in `FugueSQL`.

```{code-cell} ipython3
%%fsql
CREATE [[0,"1"]] SCHEMA a:int,b:str
SAVE OVERWRITE "/tmp/f.parquet"
SAVE OVERWRITE "/tmp/f.csv" (header=true)
SAVE OVERWRITE "/tmp/f.json"
SAVE OVERWRITE PARQUET "/tmp/f"
```

```{code-cell} ipython3
%%fsql
LOAD "/tmp/f.parquet" PRINT
LOAD "/tmp/f.parquet" COLUMNS a PRINT
LOAD PARQUET "/tmp/f" PRINT
LOAD "/tmp/f.csv" (header=true) PRINT
LOAD "/tmp/f.csv" (header=true) COLUMNS a:int,b:str PRINT
LOAD "/tmp/f.json" PRINT
LOAD "/tmp/f.json" COLUMNS a:int,b:str PRINT
```

json, csv, and parquet are support file formats. There are plans to support avro. Notice that parameters can be passed. If running on the default [execution engine](../advanced/execution_engine.ipynb), these would be passed on to **Pandas** `read_csv` and `to_csv`.  The file extension is used as a hint to use the appropriate load/save function. If the extension is not present in the filename, it has to be specified.

+++

## Variable Assignment

From here, it should be clear that `Fugue` extends SQL to make it a more complete language. One of the additional features is variable assignment. Along with this, multiple `SELECT` statements can be used. This is the equivalent of temp tables or Common Table Expressions (CTE) in SQL.

```{code-cell} ipython3
df = pd.DataFrame({"number":[0,1],"word":["hello","world"]})
```

```{code-cell} ipython3
%%fsql
-- Note that df is used from the previous Python cell
SELECT * FROM df
SAVE OVERWRITE "/tmp/f.csv"(header=true)

temp = SELECT * FROM (LOAD "/tmp/f.csv" (header=true)) 
        WHERE number = 1
output = SELECT word FROM temp
SAVE OVERWRITE "/tmp/output.csv"(header=true)

new = LOAD "/tmp/output.csv"(header=true)
PRINT new
```

## Execution Engine

So far, we've only dealt with the default [execution engine](../advanced/execution_engine.ipynb). If nothing is passed to the `%%fsql`, the `NativeExecutionEngine` is used. Like `Fugue` programming interface, the `execution engine` can be easily changed by passing it to `FugueSQLWorkflow`. Below is an example for Spark.

Take note of the output `DataFrame` in the example below. It will be a `SparkDataFrame`.

```{code-cell} ipython3
:tags: [remove-stderr]
%%fsql spark
SELECT *, 1 AS constant
FROM df
YIELD DATAFRAME AS df
```

```{code-cell} ipython3
print(type(df))
```

## Anonymity

In `FugueSQL`, one of the simplifications is anonymity. It’s optional, but it usually can significantly simplify your code and make it more readable.

For a statement that only needs to consume the previous dataframe, a `FROM` keyword is not needed. `PRINT` is the best example. `SAVE` is another example. This can be applied to other keywords. In this example we'll use the `TAKE` function that only returns the number of rows specified.

```{code-cell} ipython3
%%fsql
a = SELECT * FROM df
TAKE 2 ROWS PRESORT number DESC          # a is consumed by TAKE
PRINT 
b = SELECT * FROM df
TAKE 2 ROWS FROM b PRESORT number DESC   # equivalent explicit synax
PRINT
```

## Inline Statements

The last enhancement is inline statements. One statement can be written in another in between `(` `)` . Anonymity and variable assignment often make this unnecessary, but it's just good to know that this option exists.

```{code-cell} ipython3
%%fsql
a = CREATE [[0,"hello"], [1,"world"]] SCHEMA number:int,word:str
SELECT *
FROM (TAKE 1 ROW FROM a)
PRINT
```

## Passing DataFrames through FugueSQL cells

DataFrames in preceding `FugueSQL` cells cannot be used in future `FugueSQL` cells by default. To use them in downstream cells, the DataFrame needs to be yielded with `YIELD DATAFRAME` like in the example below. This also makes it available in Python cells. For large DataFrames, `YIELD FILE` stores the file in a temporary location for it to be loaded when used.

```{code-cell} ipython3
%%fsql
a=CREATE [[0,"hello"],[1,"world"]] SCHEMA number:int,word:str
YIELD DATAFRAME AS a
```

```{code-cell} ipython3
# Using the yielded DataFrame in Python
print(a.as_pandas().head())
```

```{code-cell} ipython3
%%fsql
b = CREATE [[0,"hello2"],[1,"world2"]] SCHEMA number:int,word2:str

SELECT a.number num, b.word2 
FROM a 
INNER JOIN b
ON a.number = b.number
PRINT
```

## From notebooks to deployment

While notebooks are good for data exploration and prototyping, some users want to include their `FugueSQL` code in Python scripts. For this, users can use the `fsql` function. Similar to `%%fsql` cells, the execution engine can be defined in the `run` method.

```{code-cell} ipython3
from fugue_sql import fsql

fsql("""
b = CREATE [[0,"hello2"],[1,"world2"]] SCHEMA number:int,word2:str

SELECT a.number num, b.word2 
FROM a 
INNER JOIN b
ON a.number = b.number
PRINT
""").run("spark")
```

In this tutorial we have gone through how to use standard SQL operations (and more) on top of Pandas, Spark, and Dask. We have also seen enhancements over standard SQL like anonymity and variable assignment.

In a [following section](python.ipynb) we'll look at more ways of integrating Python with `FugueSQL` to extend the capabilities of using SQL.
