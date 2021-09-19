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

# Output Transformer (Advanced)

`OutputTransfomer` is in general similar to `Transformer`. And any `Transformer` can be used as `OutputTransformer`. It is important to understand the difference between the operations `transform` and `out_transform`. 

* `transform` is lazy, Fugue does not ensure the compute immediately. For example, if using `SparkExecutionEngine`, the real compute of `transform` happens only when hitting an action, for example `save`.
* `out_transform` is an action, Fugue ensures the compute happening immediately, regardless of what execution engine is used.
* `transform` outputs a transformed dataframe for the following steps to use
* `out_transform` is the last compute of a branch in the DAG, it outputs nothing.

You may find that `transform().persist()` can be an alternative to `out_transform`, it's in general ok, but you must notice that, the output dataframe of a transformation can be very large, if you persist or checkpoint it, it can take up great portion of memory or disk space. In contrast, `out_transform` does not take any space. Plus, it is a more explicit way to show what you want to do. 

A typical use case of output_transform is to save the dataframe in a custom way, for example, pushing to redis.

In this tutorial are the methods to define an `OutputTransformer`. There is no preferred method and Fugue makes it flexible for users to choose whatever interface works for them. The three ways are native approach, decorator, and the class interface in order of simplicity. Note schema hints do not work.

+++

## Native Approach

An `OutputTransformer` normally returns nothing, so the default schema is `None`. Because of this, it will work if no schema is specified. The `OutputTransformer` is not meant to mutate schema so it will not respect any schema hint. 

```{code-cell} ipython3
from typing import Iterable, Dict, Any, List
from fugue import FugueWorkflow

def push_to_redis(df:Iterable[Dict[str,Any]]) -> Iterable[Dict[str,Any]]:
    for row in df:
        print("pushing1", row)
    return df

with FugueWorkflow() as dag:
    df = dag.df([[0,1],[0,2],[1,3],[1,1]],"a:int,b:int")
    df.out_transform(push_to_redis)
```

## Decorator Approach

There is no obvious advantage to use decorator for `OutputTransformer`

```{code-cell} ipython3
from fugue.extensions import output_transformer

@output_transformer()
def push_to_redis(df:Iterable[Dict[str,Any]]) -> None:
    for row in df:
        print("pushing2", row)
        continue
        
with FugueWorkflow() as dag:
    df = dag.df([[0,1],[0,2],[1,3],[1,1]],"a:int,b:int")
    df.partition(by=["a"], presort="b").out_transform(push_to_redis)
```

## Interface Approach (Advanced)

Just like the interface approach of `Transformer`, you get all the flexibilities and control over your transformation

```{code-cell} ipython3
from fugue.extensions import OutputTransformer
    
class Push(OutputTransformer):
    # Notice OutputTransformer has different interface than Transformer
    def process(self, df):
        print("pushing2", self.cursor.key_value_dict)
        
with FugueWorkflow() as dag:
    df = dag.df([[0,1],[0,2],[1,3],[1,1]],"a:int,b:int")
    df.partition(by=["a"], presort="b").out_transform(Push)
```
