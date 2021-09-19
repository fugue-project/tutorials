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

# Operators

The previous section talked about `FugueSQL` syntax. Along with the standard SQL operations, `FugueSQL` has implemented some additional keywords (and continues to add more). These keywords have equivalent methods in the programming interface. `FugueSQL` aims to make more coding fun and more English-like. Our goal is to provide an intuitive interface that is easy to read.

This is not a complete reference, it just contains the most used keywords.

```{code-cell} ipython3
from fugue_notebook import setup
setup()
```

```{code-cell} ipython3
import pandas as pd
data = [
    ["A", "2020-01-01", 10],
    ["A", "2020-01-02", None],
    ["A", "2020-01-03", 30],
    ["B", "2020-01-01", 20],
    ["B", "2020-01-02", None],
    ["B", "2020-01-03", 40]
]
data = pd.DataFrame(data, columns=["id", "date", "value"])
```

## Input and Output Operations

+++

## PRINT

Prints a dataframe.

Usage:

`PRINT [number] [ROW|ROWS] [FROM dataframe] [ROWCOUNT] [TITLE “title”]`

* dataframe - If not provided, takes the last dataframe
* number - Number of rows
* ROW|ROWS - No difference
* ROWCOUNT - Displays number of rows for dataframe. This is expensive for Spark and Dask. For distributed environments, persisting will help before doing this operation.
* TITLE - Title for display

```{code-cell} ipython3
%%fsql
-- PRINT example
df = CREATE [[0,"hello"],[1,"world"]] SCHEMA a:int,b:str
PRINT 2 ROWS FROM df TITLE "xyz"
```

## LOAD

Loads a CSV, JSON, or PARQUET file as a `DataFrame`.

Usage:

`LOAD [PARQUET|CSV|JSON] "path" (params) [COLUMNS schema|columns]`

* PARQUET|CSV|JSON - File type to load. Required if the file has no extension.
* path - File path to load.
* params - Passed on to underlying execution engine loading method.
* COLUMNS - Columns to grab or schema to load it in as.

+++

## SAVE (or SAVE AND USE)

Saves a CSV, JSON, or PARQUET file as a `DataFrame`. `SAVE AND USE` just returns the dataframe so there is no need to load it back in.

Usage:

`SAVE [dataframe] [PREPARTITION statement] [OVERWRITE|APPEND|TO] [SINGLE] [PARQUET|CSV|JSON] "path" [(params)]`

or 

`SAVE AND USE [dataframe] [PREPARTITION statement] [OVERWRITE|APPEND|TO] [SINGLE] [PARQUET|CSV|JSON] "path" [(params)]`

* dataframe - If not provided, takes the last dataframe.
* PREPARTITION - Partitions for file.
* OVERWRITE|APPEND|TO - Choose the mode for writing the file out. `TO` throws an error if the file exists.
* SINGLE - One file output.
* PARQUET|CSV|JSON - Choose file type (Parquet, CSV, or JSON) for output. Required if path has no extension.
* path - File path to write out to.
* params - Passed on to underlying execution engine saving method.

```{code-cell} ipython3
%%fsql
-- SAVE and LOAD example
CREATE [[0,"1"]] SCHEMA a:int,b:str
SAVE OVERWRITE "/tmp/f.parquet"
SAVE OVERWRITE "/tmp/f.csv" (header=true)
SAVE OVERWRITE "/tmp/f.json"
SAVE OVERWRITE PARQUET "/tmp/f"

LOAD "/tmp/f.parquet" PRINT
LOAD "/tmp/f.parquet" COLUMNS a PRINT
LOAD PARQUET "/tmp/f" PRINT
LOAD "/tmp/f.csv" (header=true) PRINT
LOAD "/tmp/f.csv" (header=true) COLUMNS a:int,b:str PRINT
LOAD "/tmp/f.json" PRINT
LOAD "/tmp/f.json" COLUMNS a:int,b:str PRINT
```

## Partitioning

Partitioning is an important part of distributed computing. Data is arranged into different logical partitions and then perform operations. This is normally used in conjunction with Fugue extensions. This is a clause that as part of statements.

+++

## PREPARTITION

Partitions a dataframe in preparation for a following operation.

Usage:

`PREPARTITION [RAND|HASH|EVEN] [number] [BY columns] [PRESORT statement]`


* RAND|HASH|EVEN - Algorithm for prepartition. Read [this](../advanced/partition.ipynb).
* number - Number of partitions.
* columns - What columns to partition on.
* statement - Presort hint. Check `PRESORT` syntax.

+++

## PRESORT

Usage:

`PRESORT column [ASC|DESC]`

Defines a presort before another operation. This is a clause mainly used with `PREPARTITION`. Multiple column, order pairs can be used separated by `,`.

* column - Name of columns to sort on.
* ASC|DESC - Order of sort.

+++

The example below shows how to use `PREPARTITION` and `PRESORT`. We need to define a transformer to apply it with.

```{code-cell} ipython3
# PREPARTITION and PRESORT example
import pandas as pd

# schema: *, shift:double
def shift(df: pd.DataFrame) -> pd.DataFrame:
    df['shift'] = df['value'].shift()
    return df
```

```{code-cell} ipython3
%%fsql
-- PREPARTITION and PRESORT example
TRANSFORM data PREPARTITION BY id PRESORT date ASC USING shift
PRINT
```

## Column and Schema Opeartions

+++

## RENAME COLUMNS

Usage:

`RENAME COLUMNS params [FROM dataframe]`

* params : Pairs of old_name:new_name separated by `,`.
* dataframe: If none is provided, take the previous one.

+++

## ALTER COLUMNS

Changes data type of columns.

Usage:

`ALTER COLUMNS params [FROM dataframe]`

* params : Pairs of column:dtype separated by `,`.
* dataframe - If not provided, takes the last one.

+++

## DROP COLUMNS

Drops columns from `DataFrame`.

Usage:

`DROP COLUMNS colnames [IF EXISTS] [FROM dataframe]`

* colnames - Column names separated by `'`.
* IF EXISTS - Drops if the column exists, otherwise error.
* dataframe - If not provided, takes the last.

```{code-cell} ipython3
%%fsql
-- RENAME COLUMNS, ALTER COLUMNS, DROP COLUMNS example
df = CREATE [[0,"1"]] SCHEMA a:int,b:str
df2 = RENAME COLUMNS a:aa, b:bb FROM df
PRINT df2
df3 = ALTER COLUMNS aa:str, bb:int FROM df2
PRINT df3
df4 = DROP COLUMNS bb, c IF EXISTS FROM df3
PRINT df4
```

## NULL Handling

+++

## DROP ROWS

Drops rows from `DataFrame` containing NULLs.

Usage:

`DROP ROWS IF ALL|ANY NULL|NULLS [ON columns] [FROM dataframe]`

* ALL|ANY - All values are NULL or any value is NULL in the row of data.
* NULL|NULLS - There is no difference.
* columns - Columns to check for NULL values.
* dataframe - If not provided, takes the last.

+++

## FILL

Fills values from `DataFrame` containing NULLs.

Usage:

`FILL NULL|NULLS PARAMS params [FROM dataframe]`

* NULL|NULLS - There is no difference
* params - Pairs of column_name:fill_value
* dataframe - If not provided, takes the last dataframe

```{code-cell} ipython3
%%fsql
-- DROP ROWS and FILL example
df = CREATE [[NULL,"1"]] SCHEMA a:double,b:str
df2 = DROP ROWS IF ANY NULL ON a FROM df
PRINT df2
df3 = DROP ROWS IF ALL NULLS FROM df
PRINT df3
df4 = FILL NULLS PARAMS a:1 FROM df
PRINT df4
```

## Sampling

+++

## SAMPLE

Takes a sample of the `DataFrame`, potentially with replacement. Use either number of rows or percent of dataframe.

Usage:

`SAMPLE [REPLACE] [rows ROWS | percent PERCENT] [SEED seed] [FROM dataframe]`

* REPLACE - Sample with replacement
* rows - Integer for number of rows.
* percent - Integer or Decimal indicating percent of dataframe to be returned
* seed - Random seed for sampling
* dataframe - If not provided, takes the last dataframe

+++

## TAKE

TAKE is equivalent to Pandas `head`. It returns the top rows of a `DataFrame`. If used with `PREPARTITION`, it returns the top rows of each partition. `PRESPORT` can be applied before taking the top rows.

Usage:

`TAKE rows ROW|ROWS [FROM dataframe ] [PREPARTITION statement] [NULL|NULLS FIRST|LAST]`

* rows - Integer for number of rows
* dataframe - If not provided, takes the last dataframe
* PREPARTITION - See syntax for `PREPARTITION`
* NULL|NULLS - No difference
* FIRST|LAST - If there is a `PRESORT`, sort with NULLS at the top or NULLS at the bottom

```{code-cell} ipython3
%%fsql
-- SAMPLE and TAKE example
df = CREATE [[1,"1"],[2,"2"],[3,"3"],[4,"4"],[5,"5"]] SCHEMA a:double,b:str
df2 = SAMPLE 2 ROWS SEED 42 FROM df
PRINT df2
df3 = SAMPLE 40 PERCENT SEED 42 FROM df
PRINT df3
df4 = TAKE 3 ROWS FROM df
PRINT df4
df5 = TAKE 1 ROW FROM df PREPARTITION BY a   # Returns 1 row for each partition
PRINT df5
```

## Distributed Computing Operations

These next keywords are used for distributed environments to save repeated computation. 

+++

## BROADCAST

Copies a DataFrame (ideally a small one) to worker nodes to prevent shuffling when joining to larger dataframes. This is used after any `FugueSQL` statement that outputs a `DataFrame`. It is used by adding it to the end of a statement.

+++

## PERSIST or CHECKPOINT

Caches a dataframe. Fugue has many types of `CHECKPOINT`. Please read [this](../advanced/checkpoint.ipynb) for a deep dive when to use each type. Similar to `BROADCAST`, it it used by appending the keyword after another `FugueSQL` statement that outputs a `DataFrame`.

```{code-cell} ipython3
%%fsql
-- BROADCAST and PERSIST example
df = CREATE [[1,"1"],[2,"2"],[3,"3"],[4,"4"],[5,"5"]] SCHEMA a:double,b:str
df2 = TAKE 2 ROWS FROM df BROADCAST
df3 = TAKE 2 ROWS FROM df PERSIST
```

## YIELD

By default, dataframes inside a `FugueSQL` cell are only local. YIELD is used to make DataFrames available in succeeding `FugueSQL` cells. There are two commands `YIELD DATAFRAME` and `YIELD FILE`. Using `YIELD DATAFRAME` holds the DataFrame in memory while `YIELD FILE` saves the file in a memory location and loads it when needed. `YIELD FILE` is intended for larger DataFrames.

```{code-cell} ipython3
%%fsql
-- YIELD example
yielded_df = CREATE [[1,"1"],[2,"2"],[3,"3"],[4,"4"],[5,"5"]] SCHEMA a:double,b:str
YIELD DATAFRAME AS yielded
```

```{code-cell} ipython3
%%fsql
-- yielded is available from the previous cell
SELECT * FROM yielded
PRINT
```
