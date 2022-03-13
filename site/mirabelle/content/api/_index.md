---
title: HTTP API
weight: 7
disableToc: false
---

## Health

- **GET** `/healthz` or `/health`

---

```
curl localhost:5558/healthz

{"message":"ok"}
```

## Metrics

- **GET** `/metrics`

Mirabelle metrics are returned as Prometheus format.

## Stream

### Add a stream

- **POST** `/api/v1/stream/<stream-name>`

The body is a json string like `{"config": "<stream-config>"}`.

<stream-config> should be the stream EDN configuration as base64. For example `{:actions {:action :info}}` as base64 would be `ezphY3Rpb25zIHs6YWN0aW9uIDppbmZvfX0=`.

You can pass the `:default` option, like in the configuration, in order to specify if you want your stream to be used by default.

You can also set the `:persist` field to `true` in your stream configuration in order to persist it on disk.
In that case, Mirabelle will write the stream configuration into the **first** directory referenced into the `:stream {:directory ..;}` configuration. The file name will be `<stream-name>.edn`.

Deleting a stream through the API will also delete its file on the filesystem.

---

```
curl -H "Content-Type: application/json" -X POST --data  '{"config": "ezphY3Rpb25zIHs6YWN0aW9uIDppbmZvfX0="}' 127.0.0.1:5558/api/v1/stream/foobar

{"message":"stream added"}
```

### List streams

- **GET** `/api/v1/stream/`

List all streams configured in Mirabelle.

---

```
 curl localhost:5558/api/v1/stream
{"streams":["bar","foo","trololo"]}
```

### Get a stream

- **GET** `/api/v1/stream/<stream-name>`

Get a stream configuration and the time of its index.

```
curl localhost:5558/api/v1/stream/trololo

{"config":"ezphY3Rpb25zIHs6YWN0aW9uIDppbmZvfSwgOmRlZmF1bHQgZmFsc2V9","current-time":0}
```

### Remove a stream

- **DELETE** `/api/v1/stream/<stream-name>`

Delete a stream by name.

If the stream was created using the API (see `Add a stream`) and persisted on disk, this call will also deleted its configuration file.

---

```
curl -X DELETE localhost:5558/api/v1/stream/trololo

{"message":"stream removed"}
```

### Push an event

- **PUT** `/api/v1/stream/<stream-name>`

Push an event to the specified stream.

---

```
curl -H "Content-Type: application/json" -X PUT --data '{"event": {"service": "foo", "metric": 10}}' 127.0.0.1:5558/api/v1/stream/foo

{"message":"ok"}
```

### Prometheus remote write

- **POST** `/api/v1/prometheus/remote-write/<stream-name>`

This endpoint can be used to send to a given Mirabelle stream Prometheus metrics. The Mirabelle endpoint should be configured into the [Prometheus configuration file](https://prometheus.io/docs/prometheus/latest/configuration/configuration/), for example:

```yaml
remote_write:
  - url: 'http://localhost:5558/api/v1/prometheus/remote-write/default'
```

The metric name will be bind in Mirabelle to the event `:service` key, anc each label will be a key as well. The event `:metric` will contain the Prometheus meric value.

### Query the index

- **GET** `/api/v1/index/<steam-name>/search`

Returns events matching the query for the index of the stream `<stream-name>`. The query passed in the body should be a [where query](/howto/stream/#filtering-events).

---

```
curl -H "Content-Type: application/json" -X POST --data '{"query": "Wzo9IDpzZXJ2aWNlICJmb28iXQ=="}' 127.0.0.1:5558/api/v1/index/default/search

{"events":[]}
```

### Get an index current time

- **GET** `/api/v1/index/<stream-name>/current-time`

Get the current time for an index

---

```
curl localhost:5558/api/v1/index/my-stream/current-time
{"current-time":0}
```
