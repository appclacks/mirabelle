---
title: Changelog
weight: 60
chapter: false
---

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
