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

# Extensions

+++

Extensions are Python functions that are wrapped in order to execute in the `%%fsql`. These are needed to implement custom logic in SQL workflows. 

+++

## Creator

`Creators` are functions that generate a `DataFrame`. The example below contains all syntax variations. Schema needs to be specified in the Python code, or in the SQL query. **Pandas** `DataFrames` have schema defined, so it does not need to be passed. The default `LOAD` an example of a `Creator`.

A common use case for `Creator` is reading from a different data source like MongoDB Atlas or AWS S3.

[Read more about Creators](../extensions/creator.ipynb)

```{code-cell}
from fugue_notebook import setup
setup()
```

```{code-cell}
from typing import List, Any
import pandas as pd

def create1(n=1) -> pd.DataFrame:
    return pd.DataFrame([[n]],columns=["a"])

# schema: a:int
def create2(n=1) -> List[List[Any]]:
    return [[n]]

def create3(n=1) -> List[List[Any]]:
    return [[n]]
```

```{code-cell}
%%fsql
CREATE [[0,"hello"],[1,"world"]] SCHEMA a:int,b:str
PRINT
CREATE USING create1 
PRINT
CREATE USING create2(n=3) 
PRINT
CREATE USING create3(n=4) SCHEMA a:int PRINT
PRINT
```

## Outputter

`Outputters` are functions that either write out `DataFrames` or display them. The default `SAVE` and `PRINT` are examples of Outputters. They do not return anything. They are invoked in SQL using the `OUTPUT` keyword.

`PREPARTITION` can be used along with `Outputters` to apply the logic on each partition. This is only possible if the `Outputter interface` is used to define the extension.

[Read more about Outputters](../extensions/outputter.ipynb)

```{code-cell}
def output(df:pd.DataFrame, n=1) -> None:
    print(n)
    print(df)
```

```{code-cell}
%%fsql
a=CREATE [[0]] SCHEMA a:int
OUTPUT a USING output(n=2)
OUTPUT PREPARTITION BY a USING output
```

## Processor

`Processors` take in multiple `DataFrames` and output one `DataFrame`. Similar to the `Outputter`, the SQL `PROCESS` keyword can be used in conjunction with `PREPARTITION` but only if the `Processor class interface` was used to define the `Processor`.

[Read more about Processors](../extensions/processor.ipynb)

```{code-cell}
def concat(df1:pd.DataFrame, df2:pd.DataFrame) -> pd.DataFrame:
    return pd.concat([df1,df2]).reset_index(drop=True)
```

```{code-cell}
%%fsql
a = CREATE [[0,"1"]] SCHEMA a:int,b:str
b = CREATE [[1,"2"]] SCHEMA a:int,b:str
PROCESS a,b USING concat
PRINT
```

## Transformer

`Transformers` are the most used extension. They take one `DataFrame` in and output one `DataFrame`. This has appeared in the previous tutorials. It can be used with `PREPARTITION` to apply the `Transformer` to each parition.

[Read more about Transformers](../extensions/transformer.ipynb)

```{code-cell}
data = [
    ["A", "2020-01-01", 10],
    ["A", "2020-01-02", None],
    ["A", "2020-01-03", 30],
    ["B", "2020-01-01", 20],
    ["B", "2020-01-02", None],
    ["B", "2020-01-03", 40]
]
df = pd.DataFrame(data, columns=["id", "date", "value"])

# schema: *, shift:double
def shift(df: pd.DataFrame) -> pd.DataFrame:
    df['shift'] = df['value'].shift()
    return df
```

```{code-cell}
%%fsql
a = SELECT * FROM df
TRANSFORM a PREPARTITION BY id PRESORT date DESC USING shift
PRINT
TRANSFORM a USING shift    # default partition
PRINT
```

**Spark** may give inconsistent results when using `TRANSFORM` without using `PREPARITION` because the default partitions are used. Also note order is not guaranteed in a distributed environment unless explicitly specified. `PREPARTITION` can also be used without a `PRESORT`. 

+++

## CoTransformer

`CoTransformers` are the used on multiple `DataFrames` parititioned in the same way. The data is then joined together with an `INNER JOIN` by default, but it can be specified which join to use. In `FugueSQL`, `TRANSFORM` and `ZIP` are used together to apply the `CoTransformer`.

[Read more about CoTransformers](../extensions/cotransformer.ipynb)

```{code-cell}
from fugue import DataFrames

#schema: res:[str]
def to_str_with_key(dfs:DataFrames) -> List[List[Any]]:
    return [[[k+" "+x.as_array().__repr__() for k,x in dfs.items()]]]
```

```{code-cell}
%%fsql
df1 = CREATE [[0,1],[1,3]] SCHEMA a:int,b:int
df2 = CREATE [[0,4],[2,2]] SCHEMA a:int,c:int
df3 = CREATE [[0,2],[1,1],[1,5]] SCHEMA a:int,d:int

TRANSFORM (ZIP df1,df2,df3) USING to_str_with_key
PRINT

TRANSFORM (ZIP a=df1,b=df2,c=df3 LEFT OUTER BY a PRESORT b DESC) USING to_str_with_key
PRINT
```
