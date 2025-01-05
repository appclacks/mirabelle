---
title: Clients and integrations
weight: 13
chapter: false
---

All [Riemann tooling](http://riemann.io/clients.html) should in theory work with Mirabelle.

## Prometheus

Mirabelle supports receiving events from [https://prometheus.io/](Prometheus). You can configure Prometheus to point to the Mirabelle HTTP API:

```
remote_write:
  - url: 'http://localhost:5558/api/v1/prometheus/remote-write/default'
```

This configuration will send Prometheus metrics too the default streams. You can send metrics to a specific stream by replacying `default` with a stream name.

The metric name will be set to the event `:name` key, the metric value to `:metric`, the timestamp to `:time`, all all labels will be set in the `:attributes` map.

## Opentelemetry traces

Mirabelle supports receiving traces through HTTP, in the [https://opentelemetry.io/docs/concepts/signals/traces/](Opentelemetry) format on the endpoint `/api/v1/opentelemetry/v1/traces/<stream-name>`.
For example, you could set `OTEL_EXPORTER_OTLP_TRACES_ENDPOINT=http://localhost:5558/api/v1/opentelemetry/v1/traces/default` to send traces to default streams in an Opentelemetry client.
