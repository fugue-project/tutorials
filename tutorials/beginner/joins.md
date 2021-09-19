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

# Joins

In the previous section, we showed how to save and load dataframes, as well as manipulate a single DataFrame. Here, we'll show how to combine DataFrames through joins. The following joins are supported in Fugue: `LEFT OUTER`, `RIGHT OUTER`, `CROSS`, `LEFT SEMI`, `LEFT ANTI`, `INNER`, `FULL OUTER`. Most of these joins should be familiar, so this section will just be about providing examples on how to use them in Fugue.

## Join and Renaming Columns

Sometimes there will be a naming conflict with columns. In such situations, the `rename` method needs to be used like the code snippet below. The `join` method takes all of the join types mentioned above for the `how` argument. `on` takes a list of columns. The columns to join on can be inferred based on the columns, but explicitly specifying is better.

```{code-cell} ipython3
from fugue import FugueWorkflow 

with FugueWorkflow() as dag:
    df1=dag.df([[0,1],[1,2]],"a:long,b:long")
    df2=dag.df([[1,1],[2,2]],"a:long,b:long")
    # we will end up with two b columns so we need to rename one
    df1.join(df2.rename({"b":"c"}),how="left_outer", on=["a"]).show()
```

## SQL vs Pandas Joins

Joins in SQL and Pandas can have different outcomes. The clearest example of this is `None` joining with `None`. In such cases, Fugue is consistent with SQL and Spark rather than Pandas. Notice that column `a` has a row with None after the join below.

```{code-cell} ipython3
import pandas as pd
df1 = pd.DataFrame({'a': [None, "a"], 'b': [1, 2]})
df2 = pd.DataFrame({'a': [None, "a"], 'b': [1, 2]})
df1.merge(df2, how="inner", on=["a", "b"])
```

With Fugue, the row with None will be dropped because it follows SQL convention.

```{code-cell} ipython3
with FugueWorkflow() as dag:
    df1=dag.df([[None,1],["a",2]],"a:str,b:long")
    df2=dag.df([[None,1],["a",2]],"a:str,b:long")
    df1.join(df2, how="inner", on=["a","b"]).show() # None,1 is excluded
```

## Multiple Joins

Multiple DataFrames can be joined together if there is no conflict.

```{code-cell} ipython3
from fugue import FugueWorkflow

with FugueWorkflow() as dag:
    df1=dag.df([[None,1],["a",2]],"a:str,b:long")
    df2=dag.df([[None,1],["a",3]],"a:str,c:long")
    df3=dag.df([[None,1],["a",4]],"a:str,d:long")
    df4=dag.df([[None,1],["a",5]],"a:str,e:long")
    df1.join(df2, df3, df4, how="inner", on=["a"]).show()
```

## Union, Intersect, Subtract

Fugue has support for Union, Intersect and Subtract. Union combines two DataFrames with the same columns. By default, only unique items are kept. Everything can be kept by setting `distinct=False`. Intersect gets the distinct elements of the intersection of the two DataFrames. Subtract gets the distinct elements of the left DataFrame that are not in the right DataFrame. Examples shown below. 

```{code-cell} ipython3
with FugueWorkflow() as dag:
    df1=dag.df([[0,1],[1,2]],"a:long,b:long")
    df2=dag.df([[0,1],[0,1],[2,2]],"a:long,b:long")
    df1.union(df2).show(title="Union")                    
    df1.union(df2, distinct=False).show(title="Union All")
    df1.intersect(df2).show(title="Intersect Distinct")
    df1.subtract(df2).show(title="Except Dictinct")
```

## Summary

This sections covers all of the base operations Fugue offers when combining two or more DataFrames. If there is logic that is not covered by this functionality, then a user can implement a custom Fugue extension and use it in a FugueWorkflow. The `transformer` we covered in previous sections is the most commonly used Fugue extension. In the next section, we'll cover the other extensions.
