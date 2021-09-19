---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.10.3
kernelspec:
  display_name: Python 3.7.9 64-bit
  name: python3
---

# Output CoTransformer (Advanced)

`OutputCoTransfomer` is similar to `CoTransformer`. And any `CoTransformer` can be used as `OutputCoTransformer`. It is important to understand the difference between the operations `transform` and `output_transform`.

Remember that the syntax to use a cotran

* `transform` is lazy meaning that Fugue does not ensure the compute immediately. For example, if using `SparkExecutionEngine`, the real compute of `transform` happens only when hitting an action, such as `print` or `save`.
* `output_transform` is an action, Fugue ensures the compute happens immediately, regardless of what execution engine is used.
* `transform` outputs a transformed dataframe for the following steps to use
* `output_transform` is the last compute of a branch in the DAG, it outputs nothing.

You may find that `transform().persist()` can be an alternative to `out_transform`, it's in general ok, but you must notice that, the output dataframe of a transformation can be very large, if you persist or checkpoint it, it can take up great portion of memory or disk space. In contrast, `out_transform` does not take any space. Plus, it is a more explicit way to show what you want to do.

In this tutorial are the methods to define an `OutputTransformer`. There is no preferred method and Fugue makes it flexible for users to choose whatever interface works for them. The three ways are native approach, decorator, and the class interface in order of simplicity. Note schema hints do not work.

A typical use case is to distributedly compare two dataframes per partition

+++

## Native Approach
An `OutputCoTransformer` normally returns nothing, so the default schema is `None`. Because of this, it will work if no schema is specified. The `OutputCoTransformer` is not meant to mutate schema so it will not respect any schema hint. 

```{code-cell} ipython3
from typing import List, Any

def assert_eq(df1:List[List[Any]], df2:List[List[Any]]) -> None:
    assert df1 == df2
    print(df1,"==",df2)

def assert_eq_2(df1:List[List[Any]], df2:List[List[Any]]) -> List[List[Any]]:
    assert df1 == df2
    print(df1,"==",df2)
    return [[0]]
```

```{code-cell} ipython3
from fugue import FugueWorkflow

with FugueWorkflow() as dag:
    df1 = dag.df([[0,1],[0,2],[1,3]], "a:int,b:int")
    df2 = dag.df([[1,3],[0,2],[0,1]], "a:int,b:int")
    z = df1.zip(df2, partition=dict(by=["a"],presort=["b"]))
    z.out_transform(assert_eq)
    z.out_transform(assert_eq_2) # All CoTransformer like functions/classes can be used directly
```

## Decorator Approach

There is no obvious advantage to use decorator for `OutputCoTransformer`

```{code-cell} ipython3
from fugue.extensions import output_cotransformer

@output_cotransformer()
def assert_eq(df1:List[List[Any]], df2:List[List[Any]]) -> None:
    assert df1 == df2
    print(df1,"==",df2)
    
with FugueWorkflow() as dag:
    df1 = dag.df([[0,1],[0,2],[1,3]], "a:int,b:int")
    df2 = dag.df([[1,3],[0,2],[0,1]], "a:int,b:int")
    z = df1.zip(df2, partition=dict(by=["a"],presort=["b"]))
    z.out_transform(assert_eq)
```

## Interface Approach

Just like the interface approach of `CoTransformer`, you get all the flexibilities and control over your transformation

```{code-cell} ipython3
from fugue.extensions import OutputCoTransformer

class AssertEQ(OutputCoTransformer):
    # notice the interface is different from CoTransformer
    def process(self, dfs):
        df1, df2 = dfs[0].as_array(), dfs[1].as_array()
        assert df1 == df2
        print(df1,"==",df2)

with FugueWorkflow() as dag:
    df1 = dag.df([[0,1],[0,2],[1,3]], "a:int,b:int")
    df2 = dag.df([[1,3],[0,2],[0,1]], "a:int,b:int")
    z = df1.zip(df2, partition=dict(by=["a"],presort=["b"]))
    z.out_transform(AssertEQ)
```
