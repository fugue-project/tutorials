# Extensions

All questions are welcome in the Slack channel.

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ)

We have mentioned graph nodes and extensions in the [DAG tutorial](../advanced/dag.ipynb). And here we present it again because this is extremely import concept in Fugue:

<img src="../../_images/extensions.svg" width="700">

* [**Creator**](./creator.ipynb): no input, single output dataframe, it is to produce dataframe input for other types of nodes, for example load file or create mock data
* [**Processor**](./processor.ipynb): one or multiple input dataframes, single output dataframe, it is to do certain transformation and pass to the next node
* [**Outputter**](./outputter.ipynb): one or multiple input dataframes, no input, it is to finalize the process of the input, for example save or print
* [**Transformer**](./transformer.ipynb): single `LocalDataFrame` in, single `LocalDataFrame` out
* [**CoTransformer**](./cotransformer.ipynb): one or multiple `LocalDataFrame` in, single `LocaDataFrame` out
* [**OutputTransformer**](./transformer.ipynb#Output-Transformer): single `LocalDataFrame` in, no output
* [**OutputCoTransformer**](./cotransformer.ipynb#Output-CoTransformer): one or multiple `LocalDataFrame` in, no output

While there are interfaces for creating these extensions, they can also be done using type hint annotations, which we call [interfaceless](./interfaceless.ipynb). 


```{toctree}
:hidden:

extensions
creator
processor
transformer
outputter
cotransformer
interfaceless
```