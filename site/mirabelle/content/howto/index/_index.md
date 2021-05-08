---
title: The index
weight: 12
disableToc: false
---

The Mirabelle Index is an in memory map which can be used to store events.

One index is instantiated and shared by all streams managed by the Mirabelle configuration file.

Streams instantiated by the API have their own index, per stream.

## How to push events into the index

```clojure
(stream
  {:name :bar :default true}
  (where [:= :service "bar"]
    (index [:host :service])))
```

In this example, all events with `:service` "bar" will be added to the index. They will be indexed by their `:host` and `:service` key.

For example, if these two events `{:host "foo" :service "bar" :time 1 :ttl 60}` and `{:host "mirabelle" :service "bar" :time 1 :ttl 60}` arrive in the system, the index would contain two keys:

`{:host "foo" :service "bar"}` -> `{:host "foo" :service "bar" :time 1 :ttl 60}`
`{:host "mirabelle" :service "bar"}` -> `{:host "mirabelle" :service "bar" :time 1 :ttl 60}`

If a new event arrives for an already existing key, the old key is overrided.

You can have multiple calls to `index` in Mirabelle, and index events based on the fields you want.

## Expiration

In Mirabelle, events have a `:time` (when the event was generated) and optionally a TTL. The default TTL for events in the index is `120` (in seconds). You could also add another default TTL using the `with` stream.

For example, an event with `:time` 1 and `:ttl` 120 would be expired at time 122.

Expiration is important. First, a lot of streams which work on windows of events will remove expired events automatically. Indeed, if you use for example [coalesce](/howto/stream/#coalesce-and-project), you don't want to keep forever the latest event for a host which does not emit anymore. When the event is expired, it's removed from the action.

Expiration can also be used to detect services which stopped emitting.

Indeed, if an event stored in the index is expired, it will be reinjected into the stream with `:state "expired"`. You could then catch it with something like:

```clojure
(expired
  (error))
```

By forwarding expired events into dedicated actions, you can for example trigger alerts based on that.

In order to trigger event expiration from events stored into the index, we should use the `reaper` action. All streams in Riemann are using the events clocks to trigger side effects, and the reaper action will update the index clock if needed, trigger events expiration, and then reinject the events into streams:

```clojure
(reaper)
(reaper :custom-stream)
```

By default, events are reinjected into the `:default` stream. You can also pass a stream name to the reaper action in order to reinject events into another stream.

I recommand you to not call forward all events to the reaper (it will impact performances). Instead, identify a couple of events arriving regularly, and forward them to the reaper in order to update the index clock.

## Queries

You can query the events stored into the index. The query should be a valid [where clause](/howto/stream/#filtering-events) in base64. For example `[:and [:> :metric 10] [:= :service "bar"]]` would be `WzphbmQgWzo+IDptZXRyaWMgMTBdIFs6PSA6c2VydmljZSAiYmFyIl1d`.

You can send queries using the Mirabelle (or Riemann) TCP clients, or using the [HTTP API](http://localhost:1313/api/#query-the-index).

## Pub Sub

Users can subscribe to the index using Websocket, in order to see in real time which events are indexed. Check the [pubsub documentation](howto/pubsub/) for more information.



