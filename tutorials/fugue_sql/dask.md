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

# FugueSQL and Dask-sql

**Pandas** and **Spark** already have solutions that allow users to execute SQL code to describe computation workflows. **Dask**, on the other hand, does not have a standard SQL interface yet. `FugueSQL` provides this feature with the DaskExecutionEngine, but users should also be aware that [dask-sql](https://dask-sql.readthedocs.io/en/latest/index.html) is a relatively new project and has a majority of SQL keywords implemented already. Additionally, it is also faster than FugueSql on average. However, there are still some features under development. Most notably, the SQL `WINDOW` is not yet implemented.

We are collaborating to have our solutions converge to create the de facto SQL interface for Dask. In the meantime, we have unified our solutions by allowing `FugueSQL` to use [dask-sql](https://dask-sql.readthedocs.io/en/latest/index.html) as an [execution engine](../advanced/execution_engine.ipynb). The [dask_sql](https://github.com/nils-braun/dask-sql) project has added a `DaskSQLExecutionEngine` into their code to let us import it and pass it into `FugueSQLWorkflow`. Note this is a different engine from `Fugue's DaskExecutionEngine`

`FugueSQLWorkflow` usage is nearly identical to the `fsql` function we saw previously. The main difference is that it takes in a SQL engine as seen in the example below.

+++

## Sample Usage

This example below shows that when the SQL query cannot be executed in `dask-sql`, it will use the `FugueSQL`. We are able to use the `TAKE` and `PRINT` keywords even if they don't exist in `dask-sql`. We can also use the `TRANSFORM and PREPARTITION` even if these are `Fugue` keywords.

`FugueSQL` and `dask-sql` together can provide a more powerful solution. This allows us to use both solutions to get the best of both worlds in terms of speed and operation completeness. All we need to do is pass the `DaskSQLExecutionEngine` into `FugueSQLWorkflow`.

NOTE: In order for the code below to run, `dask-sql` needs to be installed.

```{code-cell}
:tags: [remove-stderr]
from dask_sql.integrations.fugue import DaskSQLExecutionEngine
from fugue_sql import FugueSQLWorkflow
import pandas as pd

data = [
    ["A", "2020-01-01", 10],
    ["A", "2020-01-02", 20],
    ["A", "2020-01-03", 30],
    ["B", "2020-01-01", 20],
    ["B", "2020-01-02", 30],
    ["B", "2020-01-03", 40]
]
schema = "id:str,date:date,value:int"

# schema: *, cumsum:int
def cumsum(df: pd.DataFrame) -> pd.DataFrame:
    df["cumsum"] = df['value'].cumsum()
    return df

# Run the DAG on the DaskSQLExecutionEngine by dask-sql
with FugueSQLWorkflow(DaskSQLExecutionEngine) as dag:
    df = dag.df(data, schema)
    dag("""
    SELECT *
    FROM df
    TRANSFORM PREPARTITION BY id PRESORT date ASC USING cumsum
    TAKE 5 ROWS
    PRINT
    """) 
```

When a SQL keywords don't exist in `dask-sql`, it will default to the `Fugue DaskExecutionEngine`. However, when the keyword is registered by `dask-sql` it will use their implementation. `OVER PARITION` is registered but still being developed, which will cause errors. One workaround is to use `Fugue's TRANSFORM and PREPARTITION` like above to avoid using `OVER PARTITION` for now.

+++

---
**Conflict with SparkExecutionEngine**

Note that `dask-sql` requires Python 3.8 to run, which may cause errors with the SparkExecutionEngine because Spark is more stable on Python 3.7. 

---
