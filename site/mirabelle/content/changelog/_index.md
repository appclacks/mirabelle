---
title: Changelog
weight: 60
chapter: false
---

This is the list of user-facing changes for Mirabelle.

## v0.9.0

Fix a bug in the `mirabelle graphviz` command.

## v0.8.0

- Removal of the "disk queue" feature: I was not happy about it
- `mirabelle graphviz` subcommand to generate graphical representations of streams
- Add the stream name in all actions logging events
- Basic auth support for the HTTP server

## v0.7.0

- Add `/health` endpoint. Both `/health` and `/healthz` return the same thing.
- Improve websocket error messages on bad queries.
- Improve error messages in test mode.
- Add the ability to persist on the file system streams created through the API. It can be enabled by setting `persist` to `true` in the stream configuration.
- Add `coll-where` action. This action can be used to filter a list of events based on a condition.
- Add `coll-sort` action, which can be used to sort a collection of events based on a field.
- Add `moving-time-window` action. This window always return the events which arrived the last `duration` seconds.
- Add `ssort` action. This action buffers events for some time and send them one by one sorted to downstream actions. It can be a nice way to tolerate some late events in streams.
- Allow nesting `:or` add `and` values in conditions. For example `[:and [:or [:= :service "foo"] [:= :service "bar"]] [:> :metric 1]]` is now a value condition.

## v0.6.0

- Fix `default` action.
- Use maps instead of plain values for all actions dealing with internal states.
- Add `aggr-sum` action, which can be used to sum over time events.

## v0.5.0

- Elasticsearch I/O
- Force the use of the #secret reader for sensitive information in I/O
- throttle: add a second parameter to specify the number of events to keep
- use aero to load the configuration and support aero profiles
- include action, which can be used to reuse part of configurations in streams
- Improve error messages
- Add a new `:native?` parameter to the TCP server to enable epoll/kqueue on Netty.

## v0.4.0

- keep-keys action
- Pagerduty: avoid local timezone (thanks Toby McLaughlin)
- Small perf optimizations

## v0.3.0

- Mirabelle now build one index *per stream*.
- All actions reinjecting events into streams (reaper, reinject!) now reinject events by default on the current stream
- reaper action: add a mandatory "interval" parameter to expire events.

## v0.1.0

First release ;)
