# Debugging

This is a list of examples of common Fugue errors and the causes behind them.

[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)


```{toctree}
:hidden:

unknown_opcode
```

## Dask

### [Unknown opcode](unknown_opcode.ipynb)
Some users encounter an error like `Exception: "SystemError(\'unknown opcode\')`. This normally doesn't happen locally, but when code is brought to the cluster, this exception will get raised. This is normally due to some Python version mismatch as we'll see in this section with some advice how to diagnose it.