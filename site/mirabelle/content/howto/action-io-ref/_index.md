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
{:pagerduty-client {:config {:service-key #secret "pagerduty-service-key"
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

Forward events to the [InfluxDB](https://www.influxdata.com/) timserie database. This I/O forwards events to InfluxDB asynchronously.

```clojure
{:influxdb {:config {:connection-string "http://127.0.0.1:8086"
                     :bucket "mirabelle"
                     :org "mirabelle"
                     :measurement :service
                     ;; either use username/password
                     :username "mirabelle"
                     :password #secret "mirabelle"
                     ;; or token authenticate
                     :token #secret "my-token"
                     :default-tags {"project" "mirabelle"}
                     :tags [:service]
                     :fields [:environment]}
            :type :influxdb}}
```

The `:measurement`, `:username`, `:password`, `:token`, `:default-tags`, `:tags` and `fields` parameters are optional. The `:measurement` parameter is the event key which will be used for the influxdb measurement

Default tags will be added to all events. The `:tags` option contains the list of keys to convert to influxdb tags, and the `:fields` option for fields.

You can also add the `:influxdb/measurement`, `:influxdb/fields` and `:influxdb/tags` to your events (using the `with` action for example) in order to override per event the default configuration for these options.

### Elasticsearch

Forward events to [ElasticSearch](https://www.elastic.co/fr/). This I/O forwards events to Elasticsearch asynchronously.

```clojure
{:elastic {:type :elasticsearch
           :config {;; the list of elasticsearch osts
                    :hosts [{:address "localhost"
                             :port 9200}]
                    ;; the path to tls certificates (optional)
                    :key "/tmp/client-key.key"
                    :cert "/tmp/client-cert.crt"
                    :cacert "/tmp/cacert.crt"
                    ;; The http scheme (http or https)
                    :scheme "http"
                    ;; The default Elasticsearch index name
                    :default-index "abc"
                    ;; The default index pattern which will be added to the index
                    ;; name. optional
                    :default-index-pattern "yyyy-MM-dd"
                    ;; Timeouts options (optional)
                    :connect-timeout 1000000
                    :socket-timeout 1000000
                    ;; The number of threads for the client (optional)
                    :thread-count 3
                    ;; Basic auth configuration (optional)
                    :basic-auth {:username "name"
                                 :password #secret "pass"}
                    ;; Service token (optional)
                    :service-token #secret "my-service-token"
                    ;; API key configuration (optional)
                    :api-key {:id "id"
                              :secret #secret "secret"}}}}
```

You can set `:elasticsearch/index` to your event in order to forward an event to a specific index.

## Actions

The [generated documentation](/generated-doc/mirabelle.action.html) from the code contains explanations and examples about the available actions. Here is the list:


- [above-dt](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-above-dt)
- [aggr-max](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-aggr-max)
- [aggr-min](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-aggr-min)
- [aggr-sum](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-aggr-sum)
- [async-queue!](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-async-queue!)
- [below-dt](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-below-dt)
- [between-dt](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-between-dt)
- [changed](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-changed)
- [coalesce](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-coalesce)
- [coll-bottom](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-coll-bottom)
- [coll-count](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-coll-count)
- [coll-increase](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-coll-increase)
- [coll-max](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-coll-max)
- [coll-mean](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-coll-mean)
- [coll-min](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-coll-min)
- [coll-percentiles](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-coll-percentiles)
- [coll-quotient](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-coll-quotient)
- [coll-rate](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-coll-rate)
- [coll-sort](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-coll-sort)
- [coll-sum](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-coll-sum)
- [coll-top](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-coll-top)
- [coll-where](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-coll-where)
- [critical](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-critical)
- [critical-dt](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-critical-dt)
- [ddt](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-ddt)
- [ddt-pos](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-ddt-pos)
- [debug](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-debug)
- [decrement](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-decrement)
- [default](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-default)
- [error](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-error)
- [ewma-timeless](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-ewma-timeless)
- [exception-stream](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-exception-stream)
- [expired](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-expired)
- [extract](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-extract)
- [fixed-event-window](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-fixed-event-window)
- [fixed-time-window](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-fixed-time-window)
- [from-base64](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-from-base64)
- [include](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-include)
- [increment](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-increment)
- [index](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-index)
- [info](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-info)
- [io](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-io)
- [json-fields](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-json-fields)
- [keep-keys](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-keep-keys)
- [moving-event-window](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-moving-event-window)
- [moving-time-window](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-moving-time-window)
- [not-expired](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-not-expired)
- [outside-dt](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-outside-dt)
- [over](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-over)
- [project](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-project)
- [publish!](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-publish!)
- [push-io!](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-push-io!)
- [reaper](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-reaper)
- [reinject!](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-reinject!)
- [rename-keys](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-rename-keys)
- [scale](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-scale)
- [sdissoc](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-sdissoc)
- [sdo](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-sdo)
- [sflatten](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-sflatten)
- [sformat](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-sformat)
- [smax](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-smax)
- [smin](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-smin)
- [split](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-split)
- [ssort](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-ssort)
- [stable](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-stable)
- [tag](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-tag)
- [tagged-all](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-tagged-all)
- [tap](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-tap)
- [test-action](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-test-action)
- [throttle](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-throttle)
- [to-base64](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-to-base64)
- [under](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-under)
- [untag](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-untag)
- [warning](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-warning)
- [where](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-where)
- [with](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-with)
