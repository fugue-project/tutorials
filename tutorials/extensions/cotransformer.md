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

# CoTransformer

`CoTransformer` represents the logic unit executing on arbitrary machine on a collection of partitions of the same partition keys of the input dataframes. The partitioning logic is not a concern of `CoTransformer`, it must be specified by `zip` in the previous step. You must understand [partition](../advanced/partition.ipynb) and [zip](../advanced/execution_engine.ipynb#Zip-&-Comap)

In this tutorial are the methods to define an `CoTransformer`. There is no preferred method and Fugue makes it flexible for users to choose whatever interface works for them. The four ways are native approach, schema hint, decorator, and the class interface in order of simplicity. 

## Example Use Cases

`CoTransformer` deals with multiple DataFrames partitioned in the same way and returns one DataFrame. So in a lot of cases, it will be used for joins that are traditionally hard to do.

* **As-of merge for multiple tables**
* **Using serialized objects - model prediction per partition**

## Quick Notes on Usage

**ExecutionEngine unaware**

* `Transformers` are executed on the workers, meaning that they are not unaware of the `ExecutionEngine`.

**Acceptable input DataFrame types**

* `LocalDataFrame`, `pd.DataFrame`, `List[List[Any]]`, `Iterable[List[Any]]`, `EmptyAwareIterable[List[Any]]`, `List[Dict[str, Any]]`, `Iterable[Dict[str, Any]]`, `EmptyAwareIterable[Dict[str, Any]]`
* Can also be a single `DataFrames`

**Acceptable output DataFrame types** 

* `LocalDataFrame`, `pd.DataFrame`, `List[List[Any]]`, `Iterable[List[Any]]`, `EmptyAwareIterable[List[Any]]`, `List[Dict[str, Any]]`, `Iterable[Dict[str, Any]]`, `EmptyAwareIterable[Dict[str, Any]]`

**Explicit output schema**

* `CoTransformer` requires users to be explicit on the output schema. Different from `Transformer`, `*` is not allowed because there is a chance for column names to collide.
* Notice that `ArrayDataFrame` and other local dataframes can't be used as annotation, you must use `LocalDataFrame`.

+++

## Example Scenario - Model Predictions per Partition

`CoTransformer` is a bit hard to conceptually understand at first, so we need to clarify the example in this tutorial. First, we'll create a DataFrame with groups A,B, and C. For each of these groups, we have 2 features in `x1` and `x2`. The target variable will be `y`.

```{code-cell} ipython3
import pandas as pd
import numpy as np

np.random.seed(0)
data = pd.DataFrame({"group": (["A"]*5 + ["B"]*5 + ["C"]*5),
                    "x1": np.random.normal(0,1,15).round(3),
                    "x2": np.random.normal(0,1,15).round(3),
                    "y": np.random.normal(0,1,15).round(3)})
```

Now we make two functions. First will be `transformer` named `train_model` to train a model for each group and return it in serialized form. The serialized form is needed because Spark DataFrames can't hold Python classes unless they are serialized. Note that by the time the data enters `train_model`, it is already partitioned.

The second function is a `predict` function that will take in two DataFrames. The first will be named `data` and the second will be `models`. By the time these DataFrames come into the function, they will already have been paritioned to work on a logical group. Because we only have one model per group, `models` will actually be a DataFrame that only contains one row. It becomes really easy to access the model if we use the `List[Dict[str,Any]]` annotation. 

```{code-cell} ipython3
from typing import Dict, Any, List
from sklearn.linear_model import LinearRegression
import pickle

# schema: group:str, model:binary
def train_model(df:pd.DataFrame) -> List[List[Any]]:
    # the value of group will be the same because it's partitioned
    group = df["group"].iloc[0]
    model = LinearRegression()
    model.fit(df[["x1", "x2"]], df["y"])

    # we return the group and the model
    return [[group, pickle.dumps(model)]]

#schema: group:str, x1:double, x2:double, y_pred:double
def predict(data:pd.DataFrame, models:List[Dict[str,Any]]) -> pd.DataFrame:
    model = pickle.loads(models[0]['model'])
    data['y_pred'] = model.predict(data[["x1", "x2"]])
    return data
```

Notice that in order to pair the correct model with the appropriate group of data (A,B,C), we need to partition both DataFrames in the same way (by `group`). This is what a `CoTransformer` is meant for, operating of multiple DataFrames partitioned the same way. To demo this, we'll just apply the models on the original dataset (not advised for production but illustrative for this demo).

Before we do that, we'll just show what the model table will look like with one model for each group.

```{code-cell} ipython3
from fugue import FugueWorkflow

with FugueWorkflow() as dag:
    train = dag.df(data)                # original pd.DataFrame
    models = train.partition_by("group").transform(train_model)
    models.show()
```

## Native Approach

The simplest way, with no dependency on Fugue. You just need to have acceptable annotations on input dataframes and output. In native approach, you must specify schema in the Fugue code. We will define the `predict` function again with no annotations.

```{code-cell} ipython3
from fugue import FugueWorkflow

def predict(data:pd.DataFrame, models:List[Dict[str,Any]]) -> pd.DataFrame:
    model = pickle.loads(models[0]['model'])
    data['y_pred'] = model.predict(data[["x1", "x2"]])
    return data

out_schema = "group:str, x1:double, x2:double, y_pred:double"

with FugueWorkflow() as dag:
    train = dag.df(data)                # original pd.DataFrame
    models = train.partition_by("group").transform(train_model)

    pred = train[["group", "x1", "x2"]] # simulated test data

    # specifying partition and applying the cotransformer
    pred.zip(models, partition={"by":"group"}).transform(predict, schema=out_schema).show(5)
```

## With Schema Hint

The schema can also be provided during the function definition through the use of the schema hint comment. Providing it during definition means it does not need to be provided inside the `FugueWorkflow`.

```{code-cell} ipython3
#schema: group:str, x1:double, x2:double, y_pred:double
def predict(data:pd.DataFrame, models:List[Dict[str,Any]]) -> pd.DataFrame:
    model = pickle.loads(models[0]['model'])
    data['y_pred'] = model.predict(data[["x1", "x2"]])
    return data

with FugueWorkflow() as dag:
    train = dag.df(data)                # original pd.DataFrame
    models = train.partition_by("group").transform(train_model)

    pred = train[["group", "x1", "x2"]] # simulated test data
    pred.zip(models, partition={"by":"group"}).transform(predict).show(5)
```

## Decorator Approach

The decorator approach also has the special schema syntax and it can also take a function that generates the schema. This can be used to create new column names or types based on cotransformer parameters.

```{code-cell} ipython3
from fugue import FugueWorkflow, Schema, cotransformer
from typing import Iterable, Dict, Any, List
import pandas as pd
    
@cotransformer("group:str, x1:double, x2:double, y_pred:double")
def predict(data:pd.DataFrame, models:List[Dict[str,Any]]) -> pd.DataFrame:
    model = pickle.loads(models[0]['model'])
    data['y_pred'] = model.predict(data[["x1", "x2"]])
    return data

with FugueWorkflow() as dag:
    train = dag.df(data)                # original pd.DataFrame
    models = train.partition_by("group").transform(train_model)

    pred = train[["group", "x1", "x2"]] # simulated test data
    pred.zip(models, partition={"by":"group"}).transform(predict).show(5)
```

## Interface Approach (Advanced)

All the previous methods are just wrappers of the interface approach. They cover most of the use cases and simplify the usage. But for certain cases, implementing the interface approach significantly improves performance. Example scenarios to use the interface approach are:

* The output schema needs partition information, such as partition keys, schema, and current values of the keys.
* The transformer has an expensive but common initialization step for processing each logical partition. Initialization should then happen when initialiazing physical partition, meaning it doesn't unnecessarily repeat.

The biggest advantage of interface approach is that you can customize physical partition level initialization, and you have all the up-to-date context variables to use. In the interface approach, type annotations are not necessary, but again, it's good practice to have them.

This example will be different from the previous ones because it will demonstrate how to use context variables. For this example, we will create a new column with the model name `(model_A, model_B, modelC)`.

```{code-cell} ipython3
from fugue import CoTransformer, DataFrame, PandasDataFrame
from triad.collections import Schema
from time import sleep

class Predict(CoTransformer):
    # this is invoked on driver side
    def get_output_schema(self, dfs):
        return self.key_schema + "x1:double, x2:double, y_pred:double, model_name:str"
    
    # on initialization of the physical partition
    def on_init(self, df: DataFrame) -> None:
        sleep(0)
        
    def transform(self, dfs) -> pd.DataFrame:
        data = dfs[0].as_pandas()
        models = dfs[1].as_dict_iterable()
        model = pickle.loads(next(models)['model'])
        data['y_pred'] = model.predict(data[["x1", "x2"]])
        data['model_name'] = "model_"+self.cursor.key_value_array[0]
        return PandasDataFrame(data)
        

with FugueWorkflow() as dag:
    train = dag.df(data)                # original pd.DataFrame
    models = train.partition_by("group").transform(train_model)
    pred = train[["group", "x1", "x2"]] # simulated test data
    dag.zip(dict(data=pred,models=models), partition={"by":"group"}).transform(Predict).show(5)
```

Notice a few things here:

* How we access the key schema (`self.key_schema`), and current logical partition's keys as array (`self.cursor.key_value_array`)
* Although DataFrames is a dict, it's an ordered dict following the input order, so you can iterate in this way
* `expensive_init` is something that is a common initialization for different logical partitions, we move it to `on_init` so it will run once for each physcial partition.

+++

## Using DataFrames

Instead of using dataframes as input, you can use a single `DataFrames` for arbitrary number of inputs.

```{code-cell} ipython3
from typing import Iterable, Dict, Any, List
import pandas as pd
from fugue import DataFrames, FugueWorkflow

#schema: res:[str]
def to_str(dfs:DataFrames) -> List[List[Any]]:
    return [[[x.as_array().__repr__() for x in dfs.values()]]]

with FugueWorkflow() as dag:
    df1 = dag.df([[0,1],[1,3]],"a:int,b:int")
    df2 = dag.df([[0,4],[1,2]],"a:int,c:int")
    df3 = dag.df([[0,2],[1,1],[1,5]],"a:int,d:int")
    dag.zip(df1,df2,df3).transform(to_str).show()
    dag.zip(dict(a=df1,b=df2,c=df3)).transform(to_str).show()
```
