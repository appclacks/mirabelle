---
title: Writing streams
weight: 5
disableToc: false
---

In this section, you will learn about how streams work, how to define them and how to use them.
Not all available actions and I/O are listed here. You can see the full list in [this section of the documentation](/howto/action-io-ref/).

**Concepts**

- [What is a Mirabelle event](/howto/stream/#events)
- [What is a Mirabelle stream](/howto/stream/#streams)
- [Compilation and EDN representation of streams and compilation](/howto/stream/#edn-representation-and-compilation)
- [Include streams snippets in the main configuration, use profiles and variables](/howto/stream/#include-streams-snippets-in-the-main-configuration-profiles-and-variables)
- [Outputs and Async Queues: how to make Mirabelle communicate with external systems](/howto/stream/#outputs-and-async-queues)
- [How Mirabelle handles time](/howto/stream/#events-time)

**Writing streams**

- [Filtering events based on various fields and conditions](/howto/stream/#filtering-events)
- [Modifying events: update fields, values, set default values, select fields...](/howto/stream/#modifying-events)
- [Detect state transitions](/howto/stream/#detect-state-transitions)
- [Work with windows (time windows, events windows...)](/howto/stream/#events-windows)
- [Sort events](/howto/stream/#sort-events)
- [Action on lists of events: max, min, count, percentiles, rate...](/howto/stream/#actions-on-list-of-events)
- [Combine events from multiple hosts, correlate events between each others](/howto/stream/#coalesce-and-project)
- [The throttle action](/howto/stream/#throttle)
- [Convert a counter to a rate](/howto/stream/#convert-a-counter-to-a-rate)
- [Compute maximum and minimum values](/howto/stream/#compute-maximum-and-minimum-values)
- [Handle errors in streams](/howto/stream/#handle-exceptions-errors)
- [Discarded events](/howto/stream/#discarded-events)
- [Move events between streams](/howto/stream/#move-events-between-streams)

## Stream DSL

Mirabelle ships with a complete, extensible DSL to define streams. The DSL is heavily inspired by [Riemann](http://riemann.io/).

### Events

Events are represented as an immutable map. An event has standard fields. All fields are optional.

- `:host`: the event source. It can be an hostname for example.
- `:service`: What is measured. `http_requests_duration_seconds` for example.
- `:state`: A string representing the event state. By convention, `ok`, `warning`, `critical` or `expired` are often used.
- `:metric`: A number associated to the event (the value of what is measured).
- `:time`: The event time in second, as a timestamp (`1619988803` for example). It could also be a float (`1619988803,173413` for example), the Mirabelle/Riemann protocol supports microsecond resolution.
- `:description`: The event description.
- `:ttl`: The duration that the event is considered valid. See the [Index](/index) documentation for more information about the index and events expiration.
- `:tags`: A list of tags associated to the event (like `["foo" "bar"]` for example).
- Extra fields can also be added if you want to. One important extra field is `:stream`. It can be used to specify on which stream the event should be send. By default, events are sent to all streags with `:default` in their configurations.

### Streams

Steams have a name, and are composed by actions. Let's define a simple stream named `:log` which will log all events it receives:

```clojure
(streams
  (stream {:name :log :default true}
    (info)))
```

The `streams` action is the top level one, and will wrap all defined steams. Then, the `stream` action will define a stream. The action tapes a map as parameter which indicates the stream name in the `:name` key.
The `:default` key indicates that events arriving to the Mirabelle TCP server should be sent to this stream (multiple streams can have `:default` set, in that case events will be forwarded by default to all of these streams).

The `info` action will simply log all events flowing throught it.

Let's now define another stream:

```clojure
(streams
  (stream {:name :log :default true}
    (info))
  (stream {:name :http_requests_duration}
    (where [:= :service "http_requests_duration_seconds"]
      (info)
      (over 1.5
        (with :state "critical"
          (error))))))
```

In this second example, we still have our first stream named `:log`. We also have another stream, a bit more complex, named `:http_requests_duration`.

This second stream will first keep only events with services equal to "http_requests_duration_seconds" using the `where` action.

Then, it will log (using `info`) the events. In another branch, `over` is used
tp keep only events with `:metric` greater than `1.5` (we can imagine that
we want to alert if an http request takes longer than 1.5 seconds).

Finally, the event `:state` is set to "critical" using the `with` action, and finally the event is logged as error using `error` (we could in a real setup send
an alert to an alerting system like Pagerduty for example).

streams can have multiple branches. It's not an issue at all, modifying events in multiple branches, streams, or threads will **never** produce side effects, it's completely safe.

Now that we know how to write streams, let's use them.

### EDN representation and compilation

The Mirabelle DSL should first be compiled to an EDN datastructure before being used by Mirabelle. Let's take the previous example stream and put it in a file:

```clojure
(streams
  (stream {:name :http_requests_duration}
    (where [:= :service "http_requests_duration_seconds"]
      (info)
      (over 1.5
        (with :state "critical"
          (error))))))
```

You then need to compile this file using this command:

```
java -jar mirabelle.jar compile <source-directory-containing-your-stream> <destination-directory>
```

For example, let's say you have put the previous stream in a file named `stream.clj` in the `/tmp/streams` directory.  
If ou launch `java -jar mirabelle.jar compile /tmp/streams /tmp/compiled`, your file will be compiled and a new `stream.clj` file will be created in the destination directory (which is `/tmp/compiled` here).

Let's do that.

```
java -jar mirabelle.jar compile /tmp/streams /tmp/compiled
```

The resulting file in `/tmp/compiled/stream.clj` should be:

```clojure
{:http_requests_duration
 {:actions
  {:action :sdo,
   :children
   ({:action :where,
     :params [[:= :service "http_requests_duration_seconds"]],
     :children
     ({:action :info}
      {:action :over,
       :params [1.5],
       :children
       ({:action :with,
         :children ({:action :error}),
         :params [{:state "critical"}]})})})}}}
```

The Mirabelle DSL was compiled to an EDN representation. You can easily map what you have defined in the DSL (stream name, actions, branches...) to the generated EDN datastructure.

You are now ready to use your stream.

Let's launch Mirabelle, with the `/tmp/compiled` directory referenced into the configuration on the `:stream section` (as explained in [the configuration documentation](/howto/configuration/)).

How to launch Mirabelle is explained in [this section](/howto/build/).

Once Mirabelle started, you can send events to it. For that, you can check the [integration](/integration/) documentation section for the available clients (Riemann clients are fully compatible with Mirabelle). In this example, I will use the [Riemann C client](https://github.com/algernon/riemann-c-client) which provides a CLI and is available in many Linux package managers.

```
riemann-client send --metric-f 1 --service "http_requests_duration_seconds" --host=my-host
```

If I send the previous event, I should see in Mirabelle logs:

```json
{"@timestamp":"2021-05-01T22:48:58.786+02:00","@version":"1","message":"#riemann.codec.Event{:host \"my-host\", :service \"http_requests_duration_seconds\", :state nil, :description nil, :metric 1.0, :tags nil, :time 1.619902138786E9, :ttl nil, :x-client \"riemann-c-client\"}","logger_name":"mirabelle.action","thread_name":"defaultEventExecutorGroup-2-8","level":"INFO","level_value":20000}
```

My event was indeed logging by the `info` action in my stream. Let's send an event with the metric greater than our threshold:

```
riemann-client send --metric-f 2 --service "http_requests_duration_seconds" --host=my-host
```

You will see in the Mirabelle logs:

```json
{"@timestamp":"2021-05-01T22:50:57.960+02:00","@version":"1","message":"#riemann.codec.Event{:host \"my-host\", :service \"http_requests_duration_seconds\", :state nil, :description nil, :metric 2.0, :tags nil, :time 1.61990225796E9, :ttl nil, :x-client \"riemann-c-client\"}","logger_name":"mirabelle.action","thread_name":"defaultEventExecutorGroup-2-2","level":"INFO","level_value":20000}

{"@timestamp":"2021-05-01T22:50:57.961+02:00","@version":"1","message":"#riemann.codec.Event{:host \"my-host\", :service \"http_requests_duration_seconds\", :state \"critical\", :description nil, :metric 2.0, :tags nil, :time 1.61990225796E9, :ttl nil, :x-client \"riemann-c-client\"}","logger_name":"mirabelle.action","thread_name":"defaultEventExecutorGroup-2-2","level":"ERROR","level_value":40000}
```

The event is logged twice: one time by our `info` action, and the second time by `error` (you can see the `level` key in the log). In the second log, the `:state` was set to "critical". Our threshold works !

More examples are available at the bottom on this page, and available actions are listed in the [Actions and I/O reference](/action-io-ref/) section of the documentation.

Streams can also be created dynamically using [the API](/howto/dynamic-streams/).

Mirabelle supports hot reload on a SIGHUP. On a reload, only streams which had their configurations modified will be reloaded. Streams created using the API will be unchanged.

### Include streams snippets in the main configuration, profiles and variables

**Profiles and readers**

Mirabelle generates its configuration using the [Aero](https://github.com/juxt/aero) Clojure library.

You can set the `PROFILE` environment variable in order to use Aero [profiles](https://github.com/juxt/aero#profile):

```clojure
(streams
  (stream {:name :foo :default true}
    (where [:and
             [:= :service "disk-used"]
             [:> :metric #profile {:preprod 70
                                   :prod 60
                                   :default 90}]]
      (error))))
```

In this example, we log events as error if they have `:service` equal to "disk-used" and if the `:metric` field is greater than a threshold.

This threshold will not be the same depending of the `PROFILE` value. By defaut (if `PROFILE` is not set) the value will be `90`. If `PROFILE` is set to `preprod`, the threshold will be `70`, and `60` for `prod`.

You can also use other Aero build-in readers described in the Aero [readme](https://github.com/juxt/aero), like `#env` to read an environment variable, `#join` and `#envf` to build strings based on multiple nevironments variables...

**Include**

It's possible to include a configuration file in another one. Let's take this file named for example `log-service.clj`:

```clojure
(where [:= :service #mirabelle/var :my-service]
  (info))
```

You can then use this file using `include` in a Mirabelle stream:

```clojure
(streams
  (stream {:name :foo :default true}
    (include "log-service.clj" {:variables {:my-service "disk-used"}})
    (include "log-service.clj" {:variables {:my-service "ram-used"}})))
```

The `#mirabelle/var` reader allows you to read a variable passed to the `include` action (here, the variable is named `:my-service`).

You can also override the default Mirabelle profile (passed as an environment variable) by passing the `;profile` key to the `include` options:

```clojure
(include "log-service.clj" {:variables {:my-service "disk-used"}
                            :profile :dev})
```

The `include` action allows you to create parameterizable configuration snippets.

### outputs and async queues

In the previous example, we are only logging events, which is not very useful. What if we want to interact with other systems, like sending alerts to a service like Pagerduty, or forward all events a timeserie database like InfluxDB ?

Outputs should be defined in the `:outputs` section fo the [configuration file](/howto/configuration/). All natively available outputs are described in the [Actions and I/O reference](/action-io-ref/) section of the documentation.

For example, Mirabelle supports sending alerts to Pagerduty. Let's configure a Pagerduty client and use it in a stream.

First, modify your configuration file to create an new `:pagerduty-client` output:

```clojure
{:outputs {:pagerduty-client {:config {:service-key "pagerduty-service-key"
                                       :source-key :service
                                       :summary-keys [:host :service :state]
                                       :dedup-keys [:host :service]}
                              :type :pagerduty}}}
```


You can now use this output named `:pagerduty-client` in a stream by using the `output!` action:

```clojure
(streams
  (stream {:name :pagerduty-example}
    (output! :pagerduty-client)))
```

If this event is set to Mirabelle:

```shell
riemann-client send --metric-d 100 --service "http_requests_duration_seconds" --state "critical" --host=myhost --attribute=environment=prod
```

You should see in Pagerduty a new triggered alert named `myhost - http_requests_duration_seconds - critical` containing all the informations about your event.

You can check the [I/O documentation](/action-io-ref/) to have details about how the Pagerduty output can be used (to resolve alert automatically for example).

A special output is `async-queue`. You could define an async queue in the `:outputs` configuration key:

```clojure
{:outputs {:thread-pool {:type :async-queue
                         :config {}}}}
```

Here are the parameters you can set in the async queue `:config` map:

- `:core-pool-size`: the ThreadPoolExecutor core pool size, default to 1.
- `:max-pool-size`: the ThreadPoolExecutor max pool size, default to 8.
- `:keep-alive-time`: the time threads stays alive when unused: default to 5000 (milliseconds).
- `:queue-size`: the event queue size, default to 10000.

You can check the [javadoc](https://docs.oracle.com/en/java/javase/11/docs/api/java.base/java/util/concurrent/ThreadPoolExecutor.html) of the `ThreadPoolExecutor` class to know more about these parameters.

This will create a new async queue named `:thread-pool`. When you use an async queue in a stream, events will be pushed into the queue and downstream actions will be executed into a dedicated threadpool:

```clojure
;; we reference the async queue from the config as name
(async-queue! :thread-pool
  (some-blocking-action))
```

You can use async queues to avoid blocking the main Mirabelle threads and achieve better performance.

### Events time

In Mirabelle, **all** streams use the events time as a wall clock. All side effects (without exception), like flushing windows, will be triggered based on events time.

It means the same events, in the same order, will **always** produce the same result. It's easy for users to [write unit tests on streams](/howto/tests/) and to reason about streams thanks to this feature.

It also allows you to use Mirabelle for a lot of use cases:

- Real time stream processing
- Work with old data by replaying them (in order) on dedicated streams.

Some use cases are explained in [this section](/production/#use-cases) of the documentation.

### More examples

This section shows more advanced use cases for streams. Not all actions are described here, the list of all actions is available [here](/action-io-ref/).

#### Filtering events

The first way of filtering events is to use the `where` action. For example, `(where [:= :service "foo"]` will keep all events with service "foo".

A lot of predicates can be used in `where`:

- `:pos?`: is the value positive ? `[:pos? :metric]`
- `:neg?`: is the value negative ? `[:neg? :metric]`
- `:zero?`: is the value equal to zero ? `[:zero? :metric]`
- `:>`: is the value greater than a threshold ? `[:> :metric 10]`
- `:>=`: is the value greater or equal than a threshold ? `[:>= :metric 10]`
- `:<`: is the value lower than a threshold ? `[:< :metric 10]`
- `:<=`: is the value lower or equal than a threshold ? `[:<= :metric 10]`
- `:=`: is the value equal to the parameter ? `[:= :metric 10]`
- `:always-true`: this condition will always be true `[:always-true]`
- `:contains`: does the value contain the parameter ? `[:contains :tags "foo"]`
- `:absent`: does the value not contain the parameter ? `[:absent :tags "foo"]`
- `:regex`: is the value matching the regex ? `[:regex :service "foo.*"]`
- `:nil?`: is the value nil ? `[:nil? :host]`
- `:not-nil?`: is the value not nil ? `[:not-nil? :host]`
- `:not=`: is the value not equal to the parameter ? `[:not= :service "bar"]`

You can combine then with `:or` or `:and`. For example, `[:and [:= :service "foo"] [:> :metric 10]]` will keep all events with `:service` "foo" and `:metric` greater than 10.

The `split` action is a more powerful `where`:

```
(split
  [:> :metric 10] (debug)
  [:> :metric 5] (info)
  (error))
```

In this example, `debug` will be called if the metric is greater than 10, if not `info` is called if the metric is greater than 5, and by default `error` is called if nothing matches (the default stream is optional, the event is discarded if not set).

the `over` and `under` streams can also be used to filter events with `:metric` over or under a threshold: `(over 3)`, `(under 4)`.

You can also filter all events with `:state` "critical" using `(critical)`, filter events with `:state` "warning" using `(warning)`, and expired events using `(expired)` (`not-expired` also exists to do the opposite).

Some streams can also be used to only let pass events if a condition is true for a given period of time.
For example, the `above-dt` stream will only let events pass if all events received have their `:metric` fields above a threshold for a certain duration:

```clojure
(above-dt {:metric 1 :duration 60}
  (error)))
```

In this example, `above-dt` will let events pass (to log them as error) only if it receives events with `:metric` greater than 1 during more than 6O seconds.

The streams `below-dt`, `between-dt`, `outside-dt`, `critical-dt` also work that way. They are useful to avoid alerting on spikes for examples.

The `tagged-all` stream is also available to keep only events containing one tag or a set of tags: `(tagged-all "foo")` or `(tagged-all ["foo" "bar"])`.

#### Modifying events

A lot of actions allow you to modify events. The first one, `with`, allows you to set a field (or multiple fields) to some specific values:

```clojure
(with :state "critical"
  (info))
(with {:state "critical" :ttl 60}
  (info))
```

The `default` action is similar to `with` but it only accepts one value, and will only set the value if the value is not already defined in the event:

```clojure
(default :ttl 60
  (info))
```

The `sdissoc` action takes a field or a list of fields and will remove them from the vent. For example, `(sdissoc :host)` or `(sdissoc [:host :service])`.

You can use `rename-keys` to rename some events keys:

```clojure
(rename-keys {:host :service
              :environment :env})
```

In this example, the `:host` key will be renamed `:service` and the `:environment` key is renamed `:env`. Existing values will be overrided.

If you want to keep only some keys from an event (and so remove all the others), you can use `keep-keys`:

```clojure
(keep-keys [:host :service :time :metric :description :environment])
```

Some actions can modify the `:metric` field. `increment` and `decrement` will add +1 or -1 to it, and you can use `scale` to multiply it with a value: `(scale 1000)` for example.

You can also tags to events, for example:

```clojure
;; add the "foo" tag to events
(tag "foo")

;; add the "foo" and "bar" tags to events
(tag ["foo" "bar"])
```

#### Compute percentiles

The `percentiles` action allows you to compute percentiles (quantiles) on events:

```clojure
(percentiles {:percentiles [0.5 0.75 0.99]
              :duration 10
              :nb-significant-digits 3}
```

This example will compute the 0.5, 0.75 and 0.99 quantiles every 10 seconds. It also supports the `:delay` parameter (to tolerate events arriving late), `:highest-trackable-value` and `:lowest-discernible-value` to bound results.

The implementation uses the [HdrHistogram](https://github.com/HdrHistogram/HdrHistogram) library.

#### Compute the rate of events

You can compute rate of events (by counting them) using the `rate` action:

```clojure
(rate {:duration 20})
```

This action will send the rate of events downstream every 20 seconds. You can also add a `:delay` parameter to tolerate late events.


#### Sum events for a time period

The `:sum` stream can be used to sum events `:metrics` field for a period of time:

```clojure
(sum {:duration 20})
```

It also supports a `:delay` parameter to tolerate late events.

### Compute the mean of events for a time period

Use `mean` to compute the mean of events metrics over time:

```clojure
(mean {:duration 10}
    (info))
```

It also supports a `:delay` parameter to tolerate late events.

#### Detect state transitions

The `changed` action can be used to detect state transitions.

```clojure
(changed {:field :state :init "ok"}
  (error))
```

In this example, events will only be passed downstream to the `error` action if the `:state` value is updated, the default value being `ok`. For example:

```clojure
{:state "ok"} ;; filtered
{:state "critical"} ;; passed downstream
{:state "critical"} ;; filtered
{:state "critical"} ;; filtered
{:state "ok"} ;; passed downstream
```

You can also use the `stable` action to filter flapping states for example:

```clojure
(stable 60 :state
  (info))
```

In this example, events wll be forwarded to the child action (`info`) only if the `:state` key is stable (has the same value) for all events during 60 seconds.

#### Events Windows

You have three available window types in Mirabelle. Like some actions in Mirabelle, time windows will send downstream a list of events instead of an individual event.
It means you should be careful about which action you will use downstream. It should be actions working on list of events.

The first one, `fixed-time-window`, will buffer all events during a defined duration and then flush them downstream. For example, `(fixed-time-window {:duration 60})` will create windows of 60 seconds. You can pass a `:delay` option to `fixed-time-window` to tolerate late events and avoid losing them (for example: `(fixed-time-window {:duration 60 :delay 30})`).

The `fixed-event-window` action will created windows not based on time, but based on the number of events the action receives. For example, `(fixed-event-window {:size 60})` will buffer events until 10 are buffered, and then pass the window downstream.

The `moving-event-window` action works like `fixed-event-window` but will pass events downstream for every event received. For example, `(moving-event-window {:size 10})` will in that case always send downstream the last 10 events.

The `moving-time-window` action will return for each event all events from the last `duration` seconds, for example: `(moving-time-window {:duration 60})`.

#### Sort events

The `ssort` action can be used to

- bufferize events for a given duration
- Send then downstream one by one sorted based on a field.

Let's take for example `(ssort {:duration 10 :field :time})`. For this input:

```clojure
{:time 1} {:time 10} {:time 4} {:time 9} {:time 13} {:time 31}
```

The output would be:

```clojure
{:time 1} {:time 4} {:time 9} {:time 10} {:time 13}
```

You can pass a `:delay` option to `ssort` in order to not send events downstream immediately. For example `(ssort {:duration 10 :field :time :delay 10})` will send events downstream 10 seconds after their windows are closed.

This action can be very useful to tolerate late events in streams. You could for example use `ssort` in front of another action in order to wait for late events.

#### Actions on list of events

Several actions can be executed on list of events (produced by windows for example).

Let's take this example which creates windows of 10 events and forwards them to multiple streams:

```clojure
(fixed-event-window {:size 10}
  (coll-max
    (info))
  (coll-min
    (info))
  (coll-sum
    (info))
  (coll-quotient
    (info))
  (coll-mean
    (info))
  (coll-rate
    (info))
  (coll-count
    (info))
  (coll-top 10
    (info))
  (coll-bottom 10
    (info))
  (coll-where [:= :service "foo"]
    (info))
  (coll-sort :time
    (info)))
```

`coll-max` will forward downstream the event with the biggest `:metric` field, `coll-min` will forward the event with the smallest `:metric`.
`coll-sum` will sum all events `:metric` together.
`coll-quotient` will divide the first event `:metric` by the value of the next events.
`coll-top` and `coll-bottom` returns the events with the top biggest (or top smallest) `:metric` field (for example, `coll-top` would return the 10 events with the biggest `:metric`).
`coll-increase` should receives a list of events representing an always-increasing counter and will compute the increase between the oldest and the latest event.

`coll-mean` will compute the mean based on the event `:metric` fields. `coll-rate` compute the rate of events (the sum of all `:metrics` divided by the time range, based on the most ancient and most recent events), and `coll-count` will return a new event with `:metric` being the number of events in the window.
The three previous streams use the latest event from the list of events to build the new event.

The `coll-percentiles` action can also be used to compute percentiles on a list of events:

```clojure
(fixed-time-window {:duration 60}
  (coll-percentiles [0.5 0.75 0.98 0.99]
    (info)))
```

In this example, we generate 60-seconds time windows and pass them to the `coll-percentiles` action. The action takes que wanted quantiles as parameter.

The `coll-percentiles` action will produce for each quantile an event with the `:quantile` key set to the quantile value, and the `;metric` field set to the value computed from the list of events for this quantile. The quantiles `0` and `1` can also be used to get the smallest of biggest event.

`coll-where` will keep only events in the list matching the provided conditio (see the [filtering events](/howto/stream/#filtering-events) section.

`coll-sort` will sort events in the list based on the field passed as parameter.

If needed, you can also flatten a list of events, to get back a single event using `flatten`:

```clojure
(moving-event-window {:size 10}
  (sflatten
    (info)))
```

here, the events produced by `moving-event-window` will be sent one by one to `info`.

#### Split streams by fields

You will often need to split streams for a given field. For example, imagine you want to count the number of events emitted every 60 seconds **by host**. If you write this stream:

```clojure
(fixed-time-window {:duration 60}
  (coll-count
    (info)))
```

You will have the number of events for all hosts.

But if you write:

```clojure
(by {:fields [:host]}
  (fixed-time-window {:duration 60}
    (coll-count
      (info))))
```

The `by` action takes a map containing a `:fields` key as parameter and will generate a new instance of the downstream actions for each unique values associated to the fields. Here for example, the computation will be done in isolation for each different `:host`.

You can also pass the `:gc-interval` and `:fork-ttl` keys to the `by` action.
This will enable garbage collection, of children actions, executed every
`:gc-interval` (in seconds) and which will remove actions which didn't
receive events since `:fork-ttl` seconds.

#### Coalesce and project

These two streams can help you doing computation on events from multiple sources.

```clojure
(where [:= :service "http_requests_duration_seconds"]
  (coalesce {:duration 10 :fields [:host :environment]}
    (coll-percentiles [0.5 0.75 0.98 0.99]
      (info))))
```

`coalesce` will return periodically (here, every 10 seconds) the latest event for each, in this example, `:host` and `environment`. For example, if you have 20 unique host/environment combination push event regularly, coalesce will emit a list of 20 events (the latest for each combination).

Expired events are not emitted, so if a host stops pushing, its event will not be emitted once the event is expired.

`project` works like coalesce but instead to get the latest event based on some fields, you should provide `where` clauses:

```clojure
(project [[:= :service "enqueues"]
          [:= :service "dequeues"]]
  (coll-quotient
    (with :service "enqueues per dequeue"
      (info))))
```

In this example, we pass to project two `where` clauses, for example to divide the `:metric` from the first event with the other one.

#### Throttle

You can use the `throttle` action to let only some events pass at most every dt seconds. You can for example use it to avoid sending too many alerts to an external system:

```clojure
(throttle {:count 3 :duration 60}
  (error))
```

In this example, throttle will only forward 3 events to the `error` action every 60 seconds.

#### Convert a counter to a rate

The `dtt` action can be used to convert a counter (which could always increase) to a rate:

```clojure
(ddt
  (info))
```

If this stream is fed with `{:metric 1 :time 1}` and then `{:metric 10 :time 4}` (these events could represent the count of requests to a HTTP server for example), the event `{:metric (/ 9 3) :time 4}` will be produced. This event is computed by doing `(10-1)/3` for the `;metric` field, and the `:time` field is the time of the latest event.

The result is the rate of requests during this time period.

#### Compute maximum and minimum values

We already saw in the [Action on lists of events: max, min, count, percentiles, rate...](/howto/stream/#actions-on-list-of-events) section the `coll-max` and `coll-min` streams. You have two other ways to compute maximum and minimum values without having to build list of events.

`top` and `bottom` can be used to get the maximum or minimum events from the last seconds. For example, `(top {:duration 10})` will compute the maximum event for windows of 10 seconds. You can also configure these streams with a `:delay` value to tolerate late events (`(top {:duration 10 :delay 10})` for example).

`smax` and `smin` will send downstream *for each event they receive* the maximum or minimum event. The value is never resetted. 

#### Handle exceptions (errors)

Sometimes, streams can generate errors. For example, a downstream service can be down, or you could try to do an invalid action on an event. Let's take an example:

```clojure
(exception-stream
  (with :metric "invalid!"
    (increment))
  (error))
```

In this example, we associate a string to the event `:metric` field and then we try to incremen it. This will produce an exception.

`exception-stream` is an action which will catch all exception generated on its first child, generate an event from its exception, and pass it to its second child (`error` here). You can like that log errors, or forward them to another system.

The error event generated by exception-stream looks like this:

```clojure
{:time 1.620331421405E9,
 :service "mirabelle-exception"
 :state "error"
 :metric 1
 :tags ["exception" "java.lang.ClassCastException"]
 :exception e
 :base-event {...}
 :description "java.lang.ClassCastException: class java.lang.String cannot be cast to class java.lang.Number..."
```

The event `:time` is the time of the event which generated the exception. The `:service` will always be `mirabelle-exception`, the `:state` "error" and the `:metric` 1.

The event will have as tag "exception" and the exception class name. The `:exception` key will contain the JVM `Exception` instance, `:base-event` the full event which triggered the exception, and finally `:description` will contain a string representation of the exception (including the stacktrace).

You can extract the base event with the `extract` action, for example:

```clojure
(exception-stream
  (with :metric "invalid!"
    (increment))
  (sdo
    (error)
    (extract :base-event
      (info))))
```

#### Discarded events

If your event has the the `mirabelle/discard` tag, the event will be ignored by every action triggering side effects (logging, I/O, publish!).

#### Move events between streams

You can reinject events from a stream to itself, or to another stream (a dynamic stream for example).

By default, events are reinjected into the current stream. You can specify the name of the targeted stream if needed:

```clojure
;; send events into the current stream
(reinject!)

;; send events to another stream
(reinject! :custom-dynamic-stream)
```

Be careful about infinite loops while using `reinject!`.
