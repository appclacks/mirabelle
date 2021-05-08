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
