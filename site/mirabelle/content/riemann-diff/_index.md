---
title: Differences with Riemann
weight: 20
chapter: false
---

Mirabelle is heavily inspired by [Riemann](riemann.io), but still have major differences.

## Streams are represented as EDN

In Riemann, streams are function. In Mirabelle, they [compile](/howto/stream/#edn-representation-and-compilation) as EDN.

It means streams configurations can be easily parsed by other systems, passed between systems, verified...

## No global side effects/schedulers

In Mirabelle, **all** actions use the events' time as wall clock. It means streams times advance depending on the event they receive.

Because of that, it's easy to reason about (and test) streams: the same inputs always produce the same outputs. It's also possible to have multiple streams with different times running in parallel.

## Various streams can coexist, dynamic streams

You can in Mirabelle create streams which will not receive the events passed to the Mirabelle TCP server by default.

It means you can have some streams for "real time" computation, and some streams for other use cases. Each stream will run independently, will have its own time...

When you send an event to Mirabelle, you can specify the `stream` attribute in order to send the event to a specific stream.

## Multiple dimensions

Riemann mostly works on the `:host` and `:service` fields. it's common to encode "dimensions" in `:service`.

For example, you could have in Riemann a service named `http_requests_duration_prod_p99`. In Mirabelle, you would model this event like this: `{:service "http_requests_duration" :environment "prod" :quantile "0.99"}`

A lot of Riemann streams/actions (`index`, `coalesce`...) were rewritten. You can now provide for each action on which keys it should work instead of having to manipulate complex `:service` names.

## New actions

New actions which I think could be interesting were added to Mirabelle. Also, not all Riemann actions were backported, only the main ones (I could probably backport the other ones later if needed).

## HTTP API

Mirabelle provides an [HTTP API](/api) which allows you to manage streams dynamically (create, list, get, remove), and to gather information about your system:

- Querying the [Index](/howto/stream-index/) and the current index time
- Pushing events
- Retrieving Mirabelle metrics which are exposed using the Prometheus format.

## Query language

Mirabelle does not use the Riemann query language to query the index. Instead, it uses its [own language](/howto/stream/#filtering-events), which is used everywhere (index queries, `where` action...).

## Clear distinctions streams and I/O

Streams and I/O are configured separately, and streams references I/O. These components do not have the same lifecycle, and handling I/O is less error prone in Mirabelle, especially during reloads.

## Hot reload

In Riemann, all streams' states (time windows for example) all lost during reloads. it's not the case in Mirabelle. Only streams which were modified in the configuration are recreated and thus lose state.

## Index

In Mirabelle, each stream has its own dedicated index.

## New features

A lot of small new features were also added (like a new pubsub mechanism, the on-disk queue...).
