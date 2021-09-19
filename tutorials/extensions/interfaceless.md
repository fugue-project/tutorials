---
jupytext:
  text_representation:
    extension: .md
    format_name: myst
    format_version: 0.13
    jupytext_version: 1.10.3
kernelspec:
  display_name: Python 3
  language: python
  name: python3
---

# Interfaceless

Fugue does have interfaces for all these extensions. But in most of the cases, you don't have to implement these interfaces, instead, Fugue will adapt to your functions and wrap them under these interfaces.

One obvious benefit is that most of your code can be totally independent from Fugue. It's not like other computing frameworks, you have to implement their interfaces and use their data structures, and the dependency will spread in your code, it will be hard to move away from them, and testing is also difficult and slow.

Actually a more important benefit is that, it helps you separate your logics. What can be independent from the computing framework? What has to depend on that? How to minimize the dependency? When you use Fugue, you naturally think more about these design questions. And with the interfaceless feature, you will be able to achieve them elegantly. Even someday you decide to move away from Fugue, you should find it's simple and the Fugue mindset will still be beneficial when you move into another framework.

To be interfaceless, you must have type annotations for your python code so Fugue can understand what you want. Writing python code with type hints are in general regarded as good practice. If you are not familiar, read [PEP 483](https://www.python.org/dev/peps/pep-0483/), [PEP 484](https://www.python.org/dev/peps/pep-0484/), [typing module](https://docs.python.org/3/library/typing.html) and [mypy](http://mypy-lang.org/).

Take [**Transformer**](./transformer.ipynb) as an example:

+++



```{code-cell}
# absolutely no dependency on Fugue
from typing import List, Dict, Iterable, Any
import pandas as pd

def transformer1(df:List[List[Any]], a, b) -> List[List[Any]]:
    pass

def transformer2(df:Iterable[Dict[str, Any]], a, b) -> pd.DataFrame:
    pass

# schema: *,b:int
def transformer3(df:pd.DataFrame, a, b) -> Iterable[Dict[str, Any]]:
    pass
```

`transformer1` wants to process a partition of the dataframe in the format of list of list (row in the format of list), also return a list of list. `transformer2` will process a partition in the format of iterable of dict (row in the format of dictionary), and return a pandas dataframe. `transformer3` will be pd.DataFrame in and iterable out, which is a great design pattern for distributed computing (because it can minimize the memory usage).

Fugue is able to wrap these 3 functions to Fugue Transformers, and at runtime, the data will be passed in according to the type annotations. Also notice the comment line before `transformer3`, it is to tell Fugue the output schema of the transformation. For `transformer1` and `transformer2` you will need to specify the schema in the Fugue code.

These 3 transformers can achieve the same thing, but with the flexibility of input and output, you may write much more intuitive and less tedious code and let Fugue handle the rest.

Also with flexibile input and output, Fugue is able to optimize the execution. For example, with iterable input, Fugue will not preload the entire partition in memory, and you can exit the iteration any time. And with pd.DataFrame as input and output, you will get best performance when using [SparkExecutionEngine with pandas_udf](../advanced/useful_config.ipynb#Use-Pandas-UDF-on-SparkExecutionEngine) enabled, because pandas_udf itself requires pd.DataFrame as input and output, so your annotation eliminates data conversion.

Parameters are not required to have type annotations, but it's good practice to have annotations for all parameters.

It is fine to use class member functions as extensions.

* **Why is this useful?** You can initialize the class with certain parameters, and they can be used inside these transformers
* **What can be used:**
    * native function with comment (schema hint)
    * native function without comment (schema hint)
* **What can't be used:**
    * functions with decorator
* **What you need to be careful about?**
    * it's a bad idea to modify the class member variables inside a member function, for certain extensions and execution engine, it may not work
    * all member variables of the class are better to be simple native data types that are picklable because for certain engine such as Spark, this is required

```{code-cell}
import pandas as pd
from fugue import FugueWorkflow

class Test(object):
    def __init__(self, n):
        self.n = n
        
    # schema: *
    def transform1(self, df:pd.DataFrame) -> pd.DataFrame:
        df["a"]+=self.n
        return df
    
    # schema: *
    def transform2(self, df:pd.DataFrame) -> pd.DataFrame:
        df["a"]*=self.n
        return df
    
test = Test(5)
with FugueWorkflow() as dag:
    dag.df([[2]],"a:int").transform(test.transform1).show()
    dag.df([[2]],"a:int").transform(test.transform2).show()
```

## Different Ways to Write Extensions

Here is a general comparison on different ways to write Fugue extensions

| . | Native | With Comment | With Decorator | Interface |
| --- | ---|---|---| ---|
|Interfaceless | Yes | Yes | Yes | No |
|Independent from Fugue | Yes | Yes | No | No |
|Performance | Good | Good | Good | Slightly better |
|Function as extension | Yes | Yes | Yes | No (has to be class) |
|Fugue can use it without providing schema | Depends | Yes | Yes | Yes |
|Flexibility on constructing schema | N/A | OK | Good | Best |
|Can use class member functions | Yes | Yes | No | N/A |
