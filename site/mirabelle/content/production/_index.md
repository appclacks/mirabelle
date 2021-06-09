---
title: Mirabelle in production
weight: 2
chapter: false
---

*Warning*: Mirabelle is for now not ready for production, it's still alpha software.

Is section is about the Mirabelle streams guarantees, Mirabelle use cases, fault tolerance and the metrics exposed by Mirabelle.

## Streams guarantees and performances

Like in Riemann, Mirabelle acknowledges events. Behaviors describe in [this section](http://riemann.io/howto.html#client-backpressure-latency-and-queues) of the Riemann documentation are also true for Mirabelle (note that a couple of stuff, like some metrics, are renamed/a bit different in Mirabelle).

it's up to you to use (or not) [asynchronous queues](/howto/stream/#io-and-async-queues) if you want to.

Although Mirabelle supports receiving events on its HTTP server, please use the TCP/protobuf one if possible: performances will be *way* better.

## Use cases

Streams in Mirabelle can be either configured in [configuration files](/howto/configuration/) or using [the API](/api/).

As explained in [this section](/howto/stream/#streams) of the documentation, events received by the Mirabelle TCP server will go by default into streams which have the tag `:default`. If you pass the `stream` attribute to an event, the event will be routed into the specified stream.

For example, if I sent to the Mirabelle TCP server this event:

```clojure
{:host "foo"
 :service "bar"
 :time "1620495335"
 :stream "bar"}
```

The event will be routed to the stream named `bar`. Other streams (defined in the configuration or using this API) will not see the event.

Remember, in Mirabelle, each stream has its own "clock" based on the time of the events it receives.

Also, all streams created using the API will have a dedicated [index](/howto/index/). The streams from the configuration share the same index instance.

Mirabelle is also able to [move events between streams](/howto/stream/#move-events-between-streams).

It means you can have streams for various use cases. For example, you could have streams:

- Defined in the configuration with `:default` to `true` in order to do "real time" computation on events.
- Defined in the configuration but without `:default`, and then send events to them specifically using the Mirabelle TCP or HTTP server.
- Create streams using the API on demand. Streams with `:default true` created through the API will also receive all events flowing through the Mirabelle TCP server. You could like that enable a stream, and then remove it once you're done.

You can also perform continuous queries using Mirabelle. Indeed, if you compute things on the fly using events coming from multiple hosts (like, time windows on events coming from various hosts or services), Mirabelle may only be able to do approximation on the computations.

As previously mentioned, Mirabelle streams use the events time for their clocks, so "old" events could be removed from computations. But it's OK, because there are ways of mitigating that (accept events and actions until they are not expired, so if your events have a TTL of 60 seconds, it's OK if they arrive a few seconds late). I also like the Riemann philosophy which is `mostly correct information right now is more useful, than totally correct information only available once the failure is over`.

But what if you want totally correct information ? In that case, you can just:

- Forward all raw events to an external system (timeseries database for example)
- Reinject them a bit later, sorted, into Mirabelle. It could be a dedicated Mirabelle instance, or a dedicated stream on your first instance. In this mode, you should have totally correct computations.

You can even use Mirabelle to play with old data, like "let's reinject all data from 2 weeks ago into Mirabelle in order to compute some statistics, or detect weird things over time" !

And don't forget the [Pub Sub](/howto/pubsub/), which can really help you to see what is currently happening into Mirabelle.

## Fault tolerance

Mirabelle is not a distributed system. Like in Riemann, streams states (like time windows) and the index are lost after a crash. You could use the [On Disk queue](/howto/on-disk-queue/) in Mirabelle in order to rebuild the states but it has a lot of limitations (check the link for more information).

You can always use continuous queries (and replay events if needed) in order to achieve fault tolerance. A computation failed because the machine crashes ? Just relaunch it with the same data.

You can also put a thing like [Kafka](https://kafka.apache.org/) in front of Riemann (or even push Riemann metrics to Kafka).

I will soon add the possibility to forward events between Mirabelle instances, and I will also build a proxy which will be able to forward events to various Mirabelle instances based on some criteria/algorithms (duplicates to everyone, consistent hashing...).

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

For each async queues, you have these metrics (the `executor` label is the async queue name):

- `executor_queue_remaining_capacity`: The remaining capacity of the executor queue, example `executor_queue_remaining_capacity{executor="thread-pool",} 10000.0`.
- `executor_queue_tasks`: the accepted and completed tasks in the executor, for example `executor_queue_tasks{executor="thread-pool",state="accepted",}` or `executor_queue_tasks{executor="thread-pool",state="completed",}`
