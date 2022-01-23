---
title: Writing tests
weight: 6
disableToc: false
---

Mirabelle ships with a complete test framework for streams.

## Running tests

First, let's define and [compile](/howto/stream/#edn-representation-and-compilation) some streams:

```clojure
(streams

 (stream
  {:name :foo}
  (where [:= :service "foo"]
    (info)
    (tap :foo)))

 (stream
  {:name :bar}
  (where [:> :metric 100]
    (info)
    (tap :bar))))
```

The first stream named `:foo` will only keep all events with `:service` "foo" and log them. These events will also be registered into a `tap` named `:foo`.

A tap is a Mirabelle action which only has an effect in test mode. Events will be saved into tap, and you will be able to check what was registered on the taps.

Our second stream is also simple: it keeps all events with `:metric` greater to 100, log them, and push them into a tap named `:bar`.

Let's write a test file for these streams. The tests file should be referenced in the `:test` section of the [configuration](/howto/configuration/).

```clojure
{:test1 {:input [{:metric 10
                  :service "foo"}
                 {:metric 101
                  :service "bar"}]

         :tap-results {:foo [{:metric 10
                              :service "foo"}]
                       :bar [{:metric 101
                              :service "bar"}]}}}
```

We defined here a test named `:test1`. You could have add more tests to the map (or in another files), all tests are run in isolation.

This `:test1` key contains a map with two keys:

- `:input`: the events which will be injected into the streams
- `:tap-results`: the expected contents of the tap.

Here, we see that the tap `:foo` should contain the first event (with `:service` "foo"), and the tap `:bar` the second (with `:metric` greater than 100).

You can low run your tests:

```
export MIRABELLE_CONFIGURATION=/path/to/mirabelle/config.edn
java -jar mirabelle.jar test

All tests successful
```

If a test fails (let's say we modify the `:foo` tap result, you should get:

```clojure
1 errors
Error in test :test1
Invalid result for tap :foo
expected: [{:metric 10, :service "not-good"}]
actual: [{:metric 10, :service "foo"}]
```

Tests can also take a `:target` configuration, for example:

```clojure
{:test1 {:input [{:metric 10
                  :service "foo"}
                 {:metric 101
                  :service "bar"}]
         :target :foo
         :tap-results {:foo [{:metric 10
                              :service "foo"}]}}}
```

In this example, events in `:input` will only be injected into the `:foo` stream.

One dedicated [index](/howto/stream-index/) is created for each test.

## Things excluded from tests

In test mode, some streams behave differently than in the regular mode:

- All I/O (so `push-io` actions) are discarded.
- The `publish!` action is also discarded in test mode.
- `async-queue` actions are replaced by a simple stream which forward all events to children. It allows you to test streams using async queues without dealing with their side effects.
- Everything wrapped in `io` will be discarded. The `io` action will in test mode discard all of its children (and on regular mode, just forward events to children).


