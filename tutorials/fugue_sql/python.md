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

# Using Python

`FugueSQL` integrates with Python by creating extensions and applying them in the `%%fsql` cells. This tutorial will show an example of applying a [Fugue Transformer](../extensions/transformer.ipynb). Additionally, we'll see the simpler ways that the Python interface and `FugueSQL` interact with each other.

```{code-cell} ipython3
from fugue_notebook import setup
setup()
```

```{code-cell} ipython3
import pandas as pd
df = pd.DataFrame({"number": [0,1], "word": ["hello", "world"]})
```

## [Jinja](https://jinja.palletsprojects.com/) Templating to Insert Variables

Before going to functions, the simplest way `FugueSQL` integrates with Python is through [Jinja](https://jinja.palletsprojects.com/) templating. `DataFrames` defined previously are automatically accessible by the `DAG`. Variables on the other hand, need to be passed with [Jinja](https://jinja.palletsprojects.com/) templating.

```{code-cell} ipython3
x=0
```

```{code-cell} ipython3
%%fsql
SELECT * FROM df WHERE number={{x}}  # see we can use variable x directly
PRINT
```

## Using [Transformers](../extensions/transformer.ipynb)

`Fugue` has different [extensions](extensions.ipynb) that allow Python to interact with SQL. `Transformer` is most commonly used because it modifies that data in a dataframe. Below we create a `Transformer` in Python and apply it in the `FugueSQL`. More on `Transformer` syntax can be found here.

```{code-cell} ipython3
import re
from typing import Iterable, Dict, Any

# schema: *, vowel_count:int, consonant_count:int
def letter_count(df:Iterable[Dict[str,Any]]) -> Iterable[Dict[str,Any]]:
    for row in df:
        row['vowel_count'] = len(re.findall(r'[aeiou]', row['word'], flags=re.IGNORECASE))
        space_count = len(re.findall(r'[ -]', row['word'], flags=re.IGNORECASE))
        row['consonant_count'] = len(row['word']) - row['vowel_count'] - space_count
        yield row
```

```{code-cell} ipython3
%%fsql
SELECT * 
FROM df 
WHERE number=1
TRANSFORM USING letter_count
PRINT
```
