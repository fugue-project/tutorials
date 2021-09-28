# Examples

End to end examples of using Fugue. Any questions are welcome in the Slack channel.

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](https://join.slack.com/t/fugue-project/shared_invite/zt-jl0pcahu-KdlSOgi~fP50TZWmNxdWYQ)


```{toctree}
:hidden:

stock_sentiment
example_covid19
```

## [Stock Sentiment](stock_sentiment.ipynb)
Using Fugue to analyze stock sentiment

## [COVID-19 Exploration with FugueSQL](example_covid19.ipynb)
Analyzing COVID-19 data with FugueSQL. In this example, we show how to extend FugueSQL with Python and show some operations important for distrubted computing such as `PERSIST` and `PREPARTITION`. This example includes tips on working with big data such as testing and persisting intermediate files as parquet.