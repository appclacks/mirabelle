---
title: Changelog
weight: 60
chapter: false
---

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
