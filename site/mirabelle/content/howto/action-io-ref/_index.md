---
title: I/O and actions reference
weight: 7
disableToc: false
---

This section lists all built-in actions and I/O available in Mirabelle.

## I/O

I/O can be [referenced in streams](/howto/stream/#io-and-async-queues) in order to forward events to external systems.

### File

This I/O write all events into a file, as edn.

```clojure
{my-io-file {:config {:path "/tmp/events?edn"}
             :type :file}}
```

### Pagerduty

This I/O forwards events to [Pagerduty](https://pagerduty.com).

```clojure
{:pagerduty-client {:config {:service-key "pagerduty-service-key"
                             :source-key :service
                             :summary-keys [:host :service :state]
                             :dedup-keys [:host :service]
                             :http-options {:socket-timeout 5000
                                            :connection-timeout 5000}}
                    :type :pagerduty}}
```

- The `:service-key` parameter is your Pagerduty service (integration) key.
- `:source-key` is the event key which will be used for the alert source in the Pagerduty payload.
- `:summary-keys` is a list of keys which will be used to build the event summary. In this example, the summary would be `<event-host>-<event-service>-<event-state>`.
- `:dedup-keys` is a list of keys used to build the Pagerduty dedup key in the alert payload.
 `:http-options` is an optional map for extra HTTP options (see [clj-http](https://github.com/dakrone/clj-http) for more information).

The raw event will also be sent to Pagerduty in the `custom_details` field. The alert timestamp will be the event time, or the current time if the event has no time.

By default, the event `:state` is used to deduce the right Pagerduty action:

- "critical": trigger
- "ok": resolve

You can also set a `:pagerduty/action` key to your event in order to set the action (with the `with` action for example: `(with :pagerduty/action :trigger ...)`

### InfluxDB

Forward events to the [InfluxDB](https://www.influxdata.com/) timserie database.

```clojure
{:influxdb {:config {:connection-string "http://127.0.0.1:8086"
                     :bucket "mirabelle"
                     :org "mirabelle"
                     :measurement :service
                     ;; either use username/password
                     :username "mirabelle"
                     :password "mirabelle"
                     ;; or token authenticate
                     :token "my-token"
                     :default-tags {"project" "mirabelle"}
                     :tags [:service]
                     :fields [:environment]}
            :type :influxdb}}
```

The `:measurement`, `:username`, `:password`, `:token`, `:default-tags`, `:tags` and `fields` parameters are optional. The `:measurement` parameter is the event key which will be used for the influxdb measurement

Default tags will be added to all events. The `:tags` option contains the list of keys to convert to influxdb tags, and the `:fields` option for fields.

You can also add the `:influxdb/measurement`, `:influxdb/fields` and `:influxdb/tags` to your events (using the `with` action for example) in order to override per event the default configuration for these options.

## Actions

The [generated documentation](/generated-doc/mirabelle.action.html) from the code contains explanations about the available actions. Here is the list:

- above-dt
- async-queue!
- below-dt
- between-dt
- changed
- coalesce
- coll-count
- coll-max
- coll-mean
- coll-min
- coll-quotient
- coll-rate
- coll-sum
- critical
- critical-dt
- debug
- decrement
- ddt
- ddt-pos
- info
- error
- ewma-timeless
- exception-stream
- expired
- fixed-event-window
- fixed-time-window
- from-base64
- increment
- index
- io
- json-fields
- moving-event-window
- not-expired
- outside-dt
- over
- percentiles
- project
- publish!
- push-io!
- reaper
- reinject!
- scale
- sflatten
- split
- sdissoc
- sdo
- sformat
- tag
- tagged-all
- tap
- test-action
- throttle
- to-base64
- under
- untag
- warning
- where
- with
- restore!
