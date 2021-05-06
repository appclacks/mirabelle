---
title: Production advices
weight: 2
chapter: true
---

# Production advices

In this section, I will discuss the Mirabelle streams guarantees, metrics exposed by Mirabelle

## Metrics

Mirabelle exposes a `/metrics` endpoint. For example, a lot of JVM internal metrics are exposed. Here are describe some interesting metrics.

### TCP server

- `tcp_request_duration_seconds`: the TCP requests duration (quantiles), for example `tcp_request_duration_seconds{quantile="0.5",} 0.0`.
- `netty_event_executor_queue_size`: the Netty queue size. If this metric is high, it means your streams are not processing fast enough.

### HTTP server

- `http_request_duration_seconds`: the HTTP requests duration (quantiles), for example: `http_request_duration_seconds{method="put",uri="/api/v1/stream/streaming",quantile="0.75",} 0.002490368`

- `http_responses_total`: HTTP responses count, for example: `http_responses_total{method="put",status="200",uri="/api/v1/stream/streaming",} 1.0`

### Websocket server

- `websocket_connection_count`: Number of actives websocket connections

### Streams

- `stream_duration_seconds`: The time events pass into the streaming engine (quantiles) for example `stream_duration_seconds{quantile="0.5",} 3.4816E-5`.

### Async queues

For each async queues, you have these metrics (the `executor` label is the async quee name):

- `executor_queue_remaining_capacity`: The remaining capacity of the executor queue, example `executor_queue_remaining_capacity{executor="thread-pool",} 10000.0`.
- `executor_queue_tasks`: the accepted and completed tasks in the executor, for example `executor_queue_tasks{executor="thread-pool",state="accepted",}` or `executor_queue_tasks{executor="thread-pool",state="completed",}`
