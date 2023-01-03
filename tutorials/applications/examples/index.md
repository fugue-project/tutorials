# Examples

End to end examples of using Fugue. Have questions? Chat with us on Github or Slack:

[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)


```{toctree}
:hidden:

stock_sentiment
example_covid19
```

## [Stock Sentiment](stock_sentiment.ipynb)
Using Fugue to analyze stock sentiment

## [COVID-19 Exploration with FugueSQL](example_covid19.ipynb)
Analyzing COVID-19 data with FugueSQL. In this example, we show how to extend FugueSQL with Python and show some operations important for distrubted computing such as `PERSIST` and `PREPARTITION`. This example includes tips on working with big data such as testing and persisting intermediate files as parquet.