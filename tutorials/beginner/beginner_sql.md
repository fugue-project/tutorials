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

# FugueSQL

FugueSQL is an alternative to the Fugue Python interface. Both are used to describe your end-to-end workflow logic. The SQL semantic is platform and scale agnostic, so if you write logic in SQL, it's very high level and abstract, and the underlying computing frameworks will try to excute them in the optimal way.

The syntax of FugueSQL is between standard SQL, json and python. The goals behind this design are:

* To be fully compatible with standard SQL `SELECT` statement
* To create a seamless flow between SQL and Python coding
* To minimize syntax overhead, to make code as short as possible while still easy to read

There is a full [FugueSQL tutorial.](../fugue_sql/index.ipynb) so we will only cover the basics here. The FugueSQL tutorial is also accesible by new users.

## Hello World

To use FugueSQL, you need to make sure you have installed the SQL extra
```
pip install fugue[sql]
```

This lets us import the `%%fsql` cell magic and use `FugueSQL` cells in Jupyter notebooks

```{code-cell} ipython3
from fugue_notebook import setup
setup()
```

```{code-cell} ipython3
import pandas as pd
df = pd.DataFrame([[0,"hello"],[1,"world"]],columns = ['a','b'])
df.head()
```

FugueSQL will be mapped to the same operations of the programming interface. All ANSI SQL keywords are available in FugueSQL

```{code-cell} ipython3
%%fsql
SELECT * 
  FROM df
 WHERE a=0 
 PRINT
```

Similar to the programming interface of Fugue, we can also bring it to Spark and Dask by specifying the SQL engine.

```{code-cell} ipython3
:tags: [remove-stderr]
%%fsql spark
SELECT * 
  FROM df
 WHERE a=0 
 PRINT
```

This interface lets users implement their logic in SQL. There are also ways to combine Fugue-SQL and Fugue extensions with Python. Those will be shown in the full [Fugue SQL tutorial](../fugue_sql/index.ipynb). There is also an example of an end-to-end workflow in the [COVID-19](../examples/example_covid19.ipynb) examples
