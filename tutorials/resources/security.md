# RPC Security Guide

Have questions? Chat with us on Github or Slack:

[![Homepage](https://img.shields.io/badge/fugue-source--code-red?logo=github)](https://github.com/fugue-project/fugue)
[![Slack Status](https://img.shields.io/badge/slack-join_chat-white.svg?logo=slack&style=social)](http://slack.fugue.ai)

## Overview

Fugue's RPC (Remote Procedure Call) server enables callbacks from distributed worker nodes back to the driver during transformation execution. This is commonly used for real-time metrics reporting, progress tracking, and interactive visualizations during distributed computations.

**Important:** The Flask RPC server has no authentication and uses pickle serialization. This is intentional design that aligns with how distributed computing frameworks handle driver-executor communication.

## Security Model

### Network Isolation is the Security Boundary

Fugue's RPC security model relies on **network-level controls**, not application-level authentication. This is the same approach used by major distributed computing frameworks:

| Framework | Default Security Model |
|-----------|----------------------|
| **Spark** | `spark.authenticate=false` by default. Driver ports exposed on cluster network without authentication. |
| **Dask** | Binds to `0.0.0.0` by default with no authentication. Scheduler and workers communicate over open TCP. |
| **Ray** | No authentication by default. Head node ports accessible to worker nodes. |

### Why This Model?

Distributed computing frameworks are designed for **trusted cluster environments** where:

1. Network access is controlled by firewalls, security groups, and VPCs
2. All nodes in the cluster are running code from the same user/job
3. The cluster infrastructure itself provides isolation between tenants

## Threat Model & Risk Scenarios

### ✅ Safe Deployments (Recommended)

**Cloud-Managed Clusters:**
- AWS EMR, GCP Dataproc, Azure HDInsight: Clusters in private VPCs with security groups restricting traffic to cluster nodes only
- Databricks: Single-user clusters with network isolation

**Kubernetes:**
- Dedicated namespaces with NetworkPolicies or service mesh (Istio, Linkerd)

**On-Premise:**
- Private clusters with network segmentation (VLANs, firewalls)

### ❌ Risky Deployments (Not Recommended)

- Multi-tenant shared clusters (long-running Databricks clusters, EMR clusters with multiple teams)
- Clusters without security groups or firewall rules
- Cloud instances with `0.0.0.0/0` inbound rules on RPC ports

## Why Pickle Serialization?

The RPC server uses pickle to pass arbitrary Python objects (functions, lambdas, closures, custom classes) between workers and driver.

Example callback passing a lambda:

```python
import pandas as pd
import fugue.api as fa

# This lambda is pickled and sent to workers
callback = lambda metrics: print(f"Epoch {metrics['epoch']}: loss={metrics['loss']}")

def train_model(df: pd.DataFrame, cb: callable) -> pd.DataFrame:
    for epoch in range(10):
        # Worker pickles metrics and sends to driver
        # Driver unpickles and executes the lambda
        cb({"epoch": epoch, "loss": 0.95 ** epoch})
    return df

fa.transform(df, train_model, schema="*",
             partition={"by": "model_id"},
             engine=spark,
             callback=callback)
```

**This is the same approach Spark uses for UDF serialization** - Python UDFs are pickled, sent to executors, and unpickled for execution. Pickle deserialization can execute arbitrary code, but this is intentional - distributed computing requires executing user code.

The security question is **"who can send pickled data to the RPC server?"** Answer: only trusted cluster nodes. This is enforced at the network layer via VPCs, security groups, and firewalls.

## Deployment Best Practices

### Production Deployments

**DO:**

- **Use VPCs and private subnets** for all cluster nodes
- **Configure security groups** to allow RPC ports only from cluster CIDR blocks
- **Use dedicated clusters** per tenant/team in multi-tenant environments
- **Use cluster network DNS** - let workers resolve driver hostname instead of exposing external IPs

**DON'T:**

- Expose RPC ports to the public internet or untrusted networks
- Use shared clusters without network segmentation between users

### Development and Testing

For local development with `NativeExecutionEngine`, no RPC server is needed - callbacks execute in-process. When testing with Spark/Dask locally, bind to `127.0.0.1`.

## Configuration Reference

Configure the RPC server via engine configuration:

```python
conf = {
    "fugue.rpc.server": "fugue.rpc.flask.FlaskRPCServer",
    "fugue.rpc.flask_server.host": "0.0.0.0",      # See host options below
    "fugue.rpc.flask_server.port": "1234",
    "fugue.rpc.flask_server.timeout": "2 sec",
}

fa.transform(df, my_transform, engine=spark, engine_conf=conf, callback=my_callback)
```

### Host Options

| Host | Use Case |
|------|----------|
| `"127.0.0.1"` | Local testing only |
| `spark.conf.get("spark.driver.host")` | **Recommended for Spark** - matches Spark's driver interface |
| `"<driver-private-ip>"` | Production - specific interface |
| `"0.0.0.0"` | Binds to all interfaces (requires security groups/firewalls) |

## Summary

Fugue's RPC follows the same security model as Spark, Dask, and Ray: **network isolation over application authentication**. Most cloud deployments are already secure if using VPCs and security groups. For production Spark jobs, use `spark.driver.host` instead of `0.0.0.0`. Avoid multi-tenant shared clusters without network segmentation.
