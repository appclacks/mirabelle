(ns mirabelle.action
  (:require [aero.core :as aero]
            [cheshire.core :as json]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [corbihttp.log :as log]
            [corbihttp.spec :as spec]
            [exoscale.ex :as ex]
            [mirabelle.action.condition :as cd]
            [mirabelle.b64 :as b64]
            [mirabelle.event :as e]
            [mirabelle.io :as io]
            [mirabelle.math :as math]
            [mirabelle.pubsub :as pubsub]
            [mirabelle.spec :as mspec]
            [mirabelle.time :as time])
  (:import java.util.concurrent.Executor
           org.HdrHistogram.Histogram
           org.HdrHistogram.Recorder))

(s/def ::size pos-int?)
(s/def ::delay nat-int?)
(s/def ::duration pos-int?)
(s/def ::threshold number?)
(s/def ::high number?)
(s/def ::low number?)
(s/def ::field (s/or :keyword keyword?
                     :seq (s/coll-of keyword?)))
(s/def ::init any?)
(s/def ::fields (s/coll-of (s/or :keyword keyword?
                                 :seq (s/coll-of keyword?))))
(s/def ::count pos-int?)

(defn duration->ns
  "converts the :duration key to nanoseconds if it exists"
  [config]
  (cond-> config
    (:duration config) (update :duration time/s->ns)
    (:delay config) (update :delay time/s->ns)))

(defn select-keys-nested
  [event keyseq]
  (loop [ret {} keys keyseq]
    (if keys
      (let [key (first keys)
            seq-key? (sequential? key)
            entry (if seq-key?
                    (get-in event key ::not-found)
                    (get event key ::not-found))]
        (recur
         (if (not= entry ::not-found)
           (if seq-key?
             (assoc-in ret key entry)
             (assoc ret key entry))
           ret)
         (next keys)))
      ret)))

(defn dissoc-in
  "Dissociates an entry from a nested associative structure returning a new
  nested structure. keys is a sequence of keys. Any empty maps that result
  will not be present in the new structure."
  [m [k & ks :as keys]]
  (if ks
    (if-let [nextmap (get m k)]
      (let [newmap (dissoc-in nextmap ks)]
        (if (seq newmap)
          (assoc m k newmap)
          (dissoc m k)))
      m)
    (dissoc m k)))

(defn call-rescue
  [event children]
  (doseq [child children]
    (child event)))

(defn where*
  [_ conditions & children]
  (let [condition-fn (cd/compile-conditions conditions)]
    (fn stream [event]
      (when (condition-fn event)
        (call-rescue event children)))))

(s/def ::condition cd/valid-condition?)
(s/def ::where (s/cat :conditions ::condition))

(defn where
  "Filter events based on conditions.
  Each condition is a vector composed of the function to apply on the field,
  the field to extract from the event, and the event itself.
  Multiple conditions can be added by using `:or` or `:and`.

  ```clojure
  (where [:= :metric 4])
  ```

  Here, we keep only events where the :metric field is equal to 4.

  ```clojure
  (where [:and [:= :host \"foo\"]
               [:> :metric 10])
  ```

  Here, we keep only events with :host = foo and with :metric > 10"
  [conditions & children]
  (mspec/valid-action? ::where [conditions])
  {:action :where
   :description {:message "Filter events based on the provided condition"
                 :params (pr-str conditions)}
   :params [conditions]
   :children children})

(defn coll-where*
  [_ conditions & children]
  (let [condition-fn (cd/compile-conditions conditions)]
    (fn stream [events]
      (call-rescue (filter condition-fn events) children))))

(s/def ::coll-where (s/cat :conditions ::condition))

(defn coll-where
  "Like `where` but should receive a list of events.

  ```clojure
  (fixed-time-window {:duration 60}
    (coll-where [:and [:= :host \"foo\"]
                      [:> :metric 10]))
  ```"
  [conditions & children]
  (mspec/valid-action? ::coll-where [conditions])
  {:action :coll-where
   :description {:message "Filter a list of events based on the provided condition"
                 :params (pr-str conditions)}
   :params [conditions]
   :children children})

(defn increment*
  [_ & children]
  (fn stream [event]
    (call-rescue (update event :metric inc)
                 children)))

(defn increment
  "Increment the event :metric field.

  ```clojure
  (increment
    (debug))
  ```
  "
  [& children]
  {:action :increment
   :description {:message "Increment the :metric field"}
   :children children})

(defn decrement*
  [_ & children]
  (fn stream [event]
    (call-rescue (update event :metric dec)
                 children)))

(defn decrement
  "Decrement the event :metric field.

  ```clojure
  (decrement
    (debug))
  ```
  "
  [& children]
  {:action :decrement
   :description {:message "Decrement the :metric field"}
   :children children})

(defn log-action
  "Generic logger"
  [source-stream level]
  (let [meta {:stream (name source-stream)}]
    (fn stream [event]
      (condp = level
        :debug (log/debug meta (json/generate-string event))
        :info (log/info meta (json/generate-string event))
        :error (log/error meta (json/generate-string event))))))

(defn debug*
  [ctx]
  (log-action (:source-stream ctx) :debug))

(defn debug
  "Print the event in the logs using the debug level

  ```clojure
  (increment
    (debug))
  ```"
  []
  {:action :debug
   :description {:message "Print the event in the logs as debug"}})

(defn info*
  [ctx]
  (log-action (:source-stream ctx) :info))

(defn info
  "Print the event in the logs using the info level

  ```clojure
  (increment
    (info))
  ```"
  []
  {:action :info
   :description {:message "Print the event in the logs as info"}})

(defn error*
  [ctx]
  (log-action (:source-stream ctx) :error))

(defn error
  "Print the event in the logs using the error level

  ```clojure
  (increment
    (debug))
  ```"
  []
  {:action :error
   :description {:message "Print the event in the logs as error"}})

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn fixed-event-window*
  [_ {:keys [size]} & children]
  (let [window (atom [])]
    (fn stream [event]
      (let [events (swap! window (fn [events]
                                   (let [events (conj events event)]
                                     (if (< size (count events))
                                       [event]
                                       events))))]
        (when (= size (count events))
          (call-rescue events children))))))

(s/def ::fixed-event-window (s/cat :config (s/keys :req-un [::size])))

(defn fixed-event-window
  "Returns a fixed-sized window of events.

  ```clojure
  (fixed-event-window {:size 5}
    (debug))
  ```

  This example will return a vector events partitioned 5 by 5."
  [config & children]
  (mspec/valid-action? ::fixed-event-window [config])
  {:action :fixed-event-window
   :description {:message (format "Create a fixed event window of size %d"
                                  (:size config))}
   :params [config]
   :children children})

(defn coll-mean*
  [_ & children]
  (fn stream [events]
    (call-rescue (math/mean events) children)))

(defn coll-mean
  "Computes the events mean (on metric).
  Should receive a list of events from the previous stream.
  The most recent event is used as a base to create the new event

  ```clojure
  (fixed-event-window {:size 10}
    (coll-mean
      (debug)))
  ```

  Computes the mean on windows of 10 events"
  [& children]
  {:action :coll-mean
   :description {:message "Computes the mean of events"}
   :children children})

(defn coll-max*
  [_ & children]
  (fn stream [events]
    (call-rescue (math/max-event events) children)))

(defn coll-max
  "Returns the event with the biggest metric.
  Should receive a list of events from the previous stream.

  ```clojure
  (fixed-event-window {:size 10}
    (coll-max
      (debug)))
  ```

  Get the event the biggest metric on windows of 10 events.

  Check `top` and `smax` as well."
  [& children]
  {:action :coll-max
   :description {:message "Get the event with the biggest metric"}
   :children children})

(defn coll-quotient*
  [_ & children]
  (fn stream [events]
    (call-rescue (math/quotient events) children)))

(defn coll-quotient
  "Divide the first event `:metrìc` field by all subsequent events `:metric`.
  Return a new event containing the new `:metric`.

  Should receive a list of events from the previous stream."
  [& children]
  {:action :coll-quotient
   :description {:message "Get the event with the biggest metric"}
   :children children})

(defn coll-sum*
  [_ & children]
  (fn stream [events]
    (call-rescue (math/sum-events events) children)))

(defn coll-sum
  "Sum all the events :metric fields
  Should receive a list of events from the previous stream.

  ```clojure
  (fixed-event-window {:size 10}
    (coll-sum
      (debug)))
  ```

  Sum all :metric fields for windows of 10 events"
  [& children]
  {:action :coll-sum
   :description {:message "Get the event with the biggest metric"}
   :children children})

(defn coll-min*
  [_ & children]
  (fn stream [events]
    (call-rescue (math/min-event events) children)))

(defn coll-min
  "Returns the event with the smallest metric.
  Should receive a list of events from the previous stream.

  ```clojure
  (fixed-event-window {:size 10}
    (coll-min
      (debug)))
  ```

  Get the event the smallest metric on windows of 10 events.

  Check `bottom` and `smin` as well."
  [& children]
  {:action :coll-min
   :description {:message "Get the event with the smallest metric"}
   :children children})

(s/def ::coll-sort (s/cat :field keyword?))

(defn coll-sort*
  [_ field & children]
  (fn stream [events]
    (call-rescue (sort-by field events) children)))

(defn coll-sort
  "Sort events based on the field passed as parameter
  Should receive a list of events from the previous stream.

  ```clojure
  (fixed-event-window {:size 10}
    (coll-sort :time
      (debug)))
  ```"
  [field & children]
  (mspec/valid-action? ::coll-sort [field])
  {:action :coll-sort
   :description {:message (format "Sort events based on the field %s" field)}
   :params [field]
   :children children})

(defn test-action*
  [_ state]
  (fn stream [event]
    (swap! state conj event)))

(defn test-action
  "Bufferize all received events in the state (an atom)
  passed as parameter"
  [state & children]
  {:action :test-action
   :params [state]
   :children children})

(defn sdo*
  [_ & children]
  (fn stream [event]
    (call-rescue event children)))

(defn sdo
  "Send events to children. useful when you want to send the same
  events to multiple downstream actions.

  ```clojure
  (sdo
    (increment)
    (decrement))
  ```

  Here, events arriving in sdo will be forwarded to both increment and
  decrement.
  "
  [& children]
  {:action :sdo
   :description {:message "Forward events to children"}
   :children children})

(defn expired*
  "Keep expired events."
  [_ & children]
  (let [time-state (atom 0)]
    (fn stream [event]
      (let [current-time (swap! time-state (fn [old-time]
                                             (max old-time (:time event 0))))]
        (when (e/expired? current-time event)
          (call-rescue event children))))))

(defn expired
  "Keep expired events.

  ```clojure
  (expired
    (increment))
  ```

  In this example, all expired events will be forwarded to the `increment`stream.
  "
  [& children]
  {:action :expired
   :description {:message "Keep expired events"}
   :children children})

(defn not-expired*
  "Keep non-expired events."
  [_ & children]
  (let [time-state (atom 0)]
    (fn stream [event]
      (let [current-time (swap! time-state (fn [old-time]
                                             (max old-time (:time event 0))))]
        (when (not (e/expired? current-time event))
          (call-rescue event children))))))

(defn not-expired
  "Keep non-expired events.

  ```clojure
  (not-expired
    (increment))

  In this example, all non-expired events will be forwarded to the `increment`stream.
  ```"
  [& children]
  {:action :not-expired
   :description {:message "Remove expired events"}
   :children children})

(defn cond-dt*
  "A stream which detects if a condition `(f event)` is true during `dt` seconds.
  Takes `conditions` (like in the where action) and a time period `dt` in seconds.
  If the condition is valid for all events received during at least the period `dt`, valid events received after the `dt` period will be passed on until an invalid event arrives.
  Skips events that are too old or that do not have a timestamp."
  [_ conditions dt & children]
  (let [condition-fn (cd/compile-conditions conditions)
        last-changed-state (atom {:ok false
                                  :time nil})]
    (fn stream [event]
      (let [event-time (:time event)
            valid-event (condition-fn event)]
        (when event-time ;; filter events with no time
          (let [{ok :ok time :time}
                (swap! last-changed-state
                       (fn [{ok :ok time :time :as state}]
                         (cond
                           ;; event is validating the condition
                           ;; last event is not ok, has no time or is too old
                           ;; => last-changed-state is now ok with a new time
                           (and valid-event (and (not ok)
                                                 (or (not time)
                                                     (> event-time time))))
                           {:ok true :time event-time}
                           ;; event is not validating the condition
                           ;; => last-changed-state is now ko with no time
                           (not valid-event)
                           {:ok false :time nil}
                           ;; default value, return the state
                           :else state)))]
            (when (and ok
                       (> event-time (+ time dt)))
              (call-rescue event children))))))))

(s/def ::above-dt (s/cat :config (s/keys :req-un [::threshold ::duration])))

(defn above-dt
  "Takes a number `threshold` and a time period in seconds `duration`.
  If the condition \"the event metric is > to the threshold\" is valid for all events
  received during at least the period `duration`, valid events received after the `duration`
  period will be passed on until an invalid event arrives.
  `:metric` should not be nil (it will produce exceptions).

  ```clojure
  (above-dt {:threshold 100 :duration 10}
    (debug))
  ```

  In this example, if the events `:metric` field are greater than 100 for more than 10 seconds, events are passed downstream.
  "
  [config & children]
  (mspec/valid-action? ::above-dt [config])
  {:action :above-dt
   :description {:message
                 (format "Keep events if :metric is greater than %d during %d seconds"
                         (:threshold config)
                         (:duration config))}
   :params [[:> :metric (:threshold config)] (time/s->ns (:duration config))]
   :children children})

(s/def ::below-dt (s/cat :config (s/keys :req-un [::threshold ::duration])))

(defn below-dt
  "Takes a number `threshold` and a time period in seconds `duration`.
  If the condition `the event metric is < to the threshold` is valid for all
  events received during at least the period `duration`, valid events received after
  the `duration` period will be passed on until an invalid event arrives.
  `:metric` should not be nil (it will produce exceptions).

    ```clojure
  (below-dt {:threshold 100 :duration 10}
    (debug))
  ```

  In this example, if the events `:metric` field are lower than 100 for more than 10 seconds, events are passed downstream.
  "
  [config & children]
  (mspec/valid-action? ::below-dt [config])
  {:action :below-dt
   :description {:message
                 (format "Keep events if :metric is lower than %d during %d seconds"
                         (:threshold config)
                         (:duration config))}
   :params [[:< :metric (:threshold config)] (time/s->ns (:duration config))]
   :children children})

(s/def ::between-dt (s/cat :config (s/keys :req-un [::high ::low ::duration])))

(defn between-dt
  "Takes two numbers, `low` and `high`, and a time period in seconds, `duration`.
  If the condition `the event metric is > low and < high` is valid for all events
  received during at least the period `duration`, valid events received after the `duration`
  period will be passed on until an invalid event arrives.
  `:metric` should not be nil (it will produce exceptions).

  ```clojure
  (between-dt {:low 50 :high 100 :duration 10}
    (debug))
  ```

  In this example, if the events `:metric` field are between 50 ans 100 for more than 10 seconds, events are passed downstream.
  "
  [config & children]
  (mspec/valid-action? ::between-dt [config])
  {:action :between-dt
   :description {:message
                 (format "Keep events if :metric is between %d and %d during %d seconds"
                         (:low config)
                         (:high config)
                         (:duration config))}
   :params [[:and
             [:> :metric (:low config)]
             [:< :metric (:high config)]]
            (time/s->ns (:duration config))]
   :children children})

(s/def ::outside-dt (s/cat :config (s/keys :req-un [::low ::high ::duration])))

(defn outside-dt
  "Takes two numbers, `low` and `high`, and a time period in seconds, `duration`.
  If the condition `the event metric is < low or > high` is valid for all events
  received during at least the period `duration`, valid events received after the `duration`
  period will be passed on until an invalid event arrives.
  `:metric` should not be nil (it will produce exceptions).


  ```clojure
  (outside-dt {:low 50 :high 100 :duration 10}
    (debug))
  ```

  In this example, if the events `:metric` field are outside the 50-100 range for more than 10 seconds, events are passed downstream.
  "
  [config & children]
  (mspec/valid-action? ::outside-dt [config])
  {:action :outside-dt
   :description {:message
                 (format "Keep events if :metric is outside %d and %d during %d seconds"
                         (:low config)
                         (:high config)
                         (:duration config))}
   :params [[:or
             [:< :metric (:low config)]
             [:> :metric (:high config)]]
            (time/s->ns (:duration config))]
   :children children})

(s/def ::cond-dt (s/cat :config
                            (s/keys :req-un [::duration
                                             ::condition])))

(defn cond-dt
  "Takes a time period in seconds `duration`.
  If all events received during at least the period `duration` matches the condition, events will be passed on until

  ```clojure
  (cond-dt {:duration 10 :condition [:= :state \"error\"]}
    (debug))
  ```

  In this example, if the events `:state` are \"error\" for more than 10 seconds, events are passed downstream.
  "
  [config & children]
  (mspec/valid-action? ::cond-dt [config])
  {:action :cond-dt
   :description {:message
                 (format "Keep events if they are matching the provided condition %s for %d seconds"
                         (str (:condition config))
                         (:duration config))}
   :params [(:condition config)
            (time/s->ns (:duration config))]
   :children children})

(defn default*
  [_ field value & children]
  (let [get-fn (if (sequential? field)
                 get-in
                 get)
        assoc-fn (if (sequential? field)
                   assoc-in
                   assoc)]
    (fn stream [event]
      (if-not (get-fn event field)
        (call-rescue (assoc-fn event field value) children)
        (call-rescue event children)))))

(s/def ::default (s/cat :field (s/or :any mspec/not-null
                                     :seq (s/coll-of mspec/not-null))
                        :value any?))

(defn default
  "Set a default value for an event

  ```clojure
  (default :state \"ok\"
    (info))
  ```

  In this example, all events where `:state` is not set will be updated with
  `:state` to \"ok\".

  It also supports nested keys:

  ```clojure
  (default [:nested :key] \"ok\"
    (info))
  ```"
  [field value & children]
  (mspec/valid-action? ::default [field value])
  {:action :default
   :description {:message (format "Set (if nil) %s to %s" field (str value))}
   :params [field value]
   :children children})

(defn output!*
  [context output-name]
  ;; discard io in test mode
  (if (:test-mode? context)
    (fn stream [_] nil)
    (if-let [output-component (get-in context [:outputs output-name :component])]
      (fn stream [event]
        (io/inject! output-component event))
      (throw (ex/ex-incorrect (format "Output %s not found"
                                      output-name))))))

(s/def ::output! (s/cat :output-name keyword?))

(defn output!
  "Push events to an external system.

  Outputs are configured into the main Mirabelle configuration file.
  If you create a new output named `:elasticsearch`
  for example, you can use output! to push all events into this I/O:

  ```clojure
  (output! :elasticsearch)
  ```

  Outputs are automatically discarded in test mode."
  [output-name]
  (mspec/valid-action? ::output! [output-name])
  {:action :output!
   :description {:message (format "Forward events to the output %s" output-name)}
   :params [output-name]})

(defn coalesce*
  [_ {:keys [duration fields]} & children]
  (let [state (atom {:buffer {}
                     :current-time 0
                     :last-tick nil
                     :window nil})
        key-fn #(vals (select-keys-nested % fields))]
    ;; the implementation can probably be optimized ?
    (fn stream [event]
      (let [buffer-update-fn (fn [current-event]
                               (cond
                                 ;; current-event is nil
                                 (not current-event)
                                 event

                                 ;; current event most recent
                                 (e/most-recent? current-event
                                                 event)
                                 current-event
                                 :else
                                 event))

            current-state
            (swap! state
                   (fn [{:keys [current-time last-tick buffer] :as state}]
                     ;; remove events with no time
                     (if (nil? (:time event))
                       (assoc state :window nil)
                       (let [current-time (max current-time
                                               (:time event))]
                         (cond

                           ;; event expired, don't keep it
                           (e/expired? current-time event)
                           (assoc state :window nil)

                           ;; to last tick, set it to the current time
                           ;; and keep the event
                           (nil? last-tick)
                           (-> (update-in state
                                          [:buffer (key-fn event)]
                                          buffer-update-fn)
                               (assoc :last-tick (:time event)
                                      :window nil))

                           ;; we are still in the same window, add the event
                           ;; to the buffer
                           (< current-time (+ last-tick duration))
                           (-> (update-in state
                                          [:buffer (key-fn event)]
                                          buffer-update-fn)
                               (assoc :window nil
                                      :current-time current-time))

                           ;; we should emit
                           :else
                           (let [tmp-buffer (->> (update buffer
                                                         (key-fn event)
                                                         buffer-update-fn)
                                                 (remove (fn [[_ v]]
                                                           (e/expired? current-time v))))]
                             (-> (assoc state
                                        :last-tick current-time
                                        :current-time current-time
                                        :buffer (into {} tmp-buffer)
                                        :window (map second tmp-buffer)))))))))]
        (when-let [window (:window current-state)]
          (call-rescue window children))))))

(s/def ::coalesce (s/cat :config (s/keys :req-un [::duration ::fields])))

(defn coalesce
  "Returns a list of the latest non-expired events (by `fields`) every dt seconds
  (at best).

  ```clojure
  (coalesce {:duration 10 :fields [:host :service]}
    (debug)
  ```

  In this example, the latest event for each host/service combination will be
  kept and forwarded downstream. The `debug` action will then receive this list
  of events.
  Expired events will be removed from the list.

  coalesce supports nested fields:

  ```clojure
  (coalesce {:duration 10 :fields [:host [:nested :field]]}
    (debug)
  ```
  "
  [config & children]
  (mspec/valid-action? ::coalesce [config])
  {:action :coalesce
   :description {:message
                 (format "Returns a list of the latest non-expired events for each fields (%d) combinations, every %s seconds"
                         (:duration config)
                         (pr-str (:fields config)))}
   :children children
   :params [(duration->ns config)]})

(defn with*
  [_ fields & children]
  (fn stream [event]
    (call-rescue
     (reduce (fn [event [k v]]
               (if (sequential? k)
                 (assoc-in event k v)
                 (assoc event k v)))
             event
             fields)
     children)))

(defn with
  "Set an event field to the given value.

  ```clojure
  (with :state \"critical\"
    (debug))
  ```

  This example set the field `:state` to \"critical\" for events.

  A map can also be provided:

  ```clojure
  (with {:service \"foo\" :state \"critical\"}
    (debug))
  ```

  This example set the the field `:service` to \"foo\" and the field `:state`
  to \"critical\" for events.

  This action also supports updated nested keys:

  ```clojure
  (with [:nested :key] \"critical\"
    (debug))
  ```

  ```clojure
  (with {[:nested :key] \"critical\"}
    (debug))
  ```"
  [& args]
  (cond
    (map? (first args))
    {:action :with
     :description {:message "Merge the events with the provided fields"
                   :params (pr-str (first args))}
     :children (rest args)
     :params [(first args)]}

    :else
    (let [[k v & children] args]
      (when (or (not k) (not v))
        (throw (ex/ex-info (format "Invalid parameters for with: %s %s" k v)
                           {})))
      {:action :with
       :description {:message (format "Set the field %s to %s" k (str v))}
       :children children
       :params [{k v}]})))

(defn coll-rate*
  [_  & children]
  (fn stream [events]
    (call-rescue (math/rate events) children)))

(defn coll-rate
  "Computes the rate on a list of events.
  Should receive a list of events from the previous stream.
  The latest event is used as a base to build the new event.

  ```clojure
  (fixed-event-window {:size 3}
    (coll-rate
      (debug)))
  ```

  If this example receives the events:

  {:metric 1 :time 1} {:metric 2 :time 2} {:metric 1 :time 3}

  The stream will return {:metric 2 :time 3}

  Indeed, (1+2+1)/2 = 3 (we divide by 2 because we have 2 seconds between the
  min and max events time).
  "
  [& children]
  {:action :coll-rate
   :description {:message "Takes a list of events and computes their rates"}
   :children children})

(defn sflatten*
  [_ & children]
  (fn stream [events]
    (doseq [e events]
      (call-rescue e children))))

(defn sflatten
  "Streaming flatten. Calls children with each event in events.
  Events should be a sequence.

  This stream can be used to \"flat\" a sequence of events (emitted
  by a time window stream for example).

  ```clojure
  (fixed-event-window {:size 5}
    (sflatten
      (info)))
  ```"
  [& children]
  {:action :sflatten
   :description {:message "Send events from a list downstream one by one"}
   :children children})

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn tag*
  [_ tags & children]
  (let [tags (flatten [tags])]
    (fn stream [event]
      (call-rescue
       (assoc event :tags (distinct (concat tags (:tags event))))
       children))))

(s/def ::tag (s/cat :tags (s/or :single string?
                                :multiple (s/coll-of string?))))

(defn tag
  "Adds a new tag, or set of tags, to events which flow through.

  ```clojure
  (tag \"foo\"
    (info))
  ```

  This example adds the tag \"foo\" to events.

  ```clojure
  (tag [\"foo\" \"bar\"] (info))
  ```

  This example adds the tag \"foo\" and \"bar\" to events."
  [tags & children]
  (mspec/valid-action? ::tag [tags])
  {:action :tag
   :description {:message (str "Tag events with %s" tags)}
   :params [tags]
   :children children})

(defn untag*
  [_ tags & children]
  (let [tags (set (flatten [tags]))
        blacklist #(not (tags %))]
    (fn stream [event]
      (call-rescue (update event :tags #(filter blacklist %))
                   children))))

(s/def ::untag (s/cat :tags (s/or :single string?
                                  :multiple (s/coll-of string?))))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn untag
  "Removes a tag, or set of tags, from events which flow through.

  ```clojure
  (untag \"foo\" (debug))
  ```

  This example removes the tag \"foo\" from events.

  ```clojure
  (untag [\"foo\" \"bar\"] (debug))
  ```

  This example removes the tags \"foo\" and \"bar\" from events"
  [tags & children]
  (mspec/valid-action? ::untag [tags])
  {:action :untag
   :description {:message (str "Remove tags " tags)}
   :params [tags]
   :children children})

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn tagged-all*
  [_ tags & children]
  (let [tag-coll (set (flatten [tags]))]
    (fn stream [event]
      (when (e/tagged-all? tag-coll event)
        (call-rescue event children)
        true))))

(s/def ::tagged-all (s/cat :tags (s/or :single string?
                                       :multiple (s/coll-of string?))))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn tagged-all
  "Passes on events where all tags are present.

  ```clojure
  (tagged-all \"foo\"
    (info))
  ```

  This example keeps only events tagged \"foo\".

  ```clojure
  (tagged-all [\"foo\" \"bar\"] (info))
  ```

  This example keeps only events tagged \"foo\" and \"bar\".
  "
  [tags & children]
  (mspec/valid-action? ::tagged-all [tags])
  {:action :tagged-all
   :description {:message (str "Keep only events with tagged " tags)}
   :params [tags]
   :children children})

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn ddt*
  [_ remove-neg? & children]
  (let [prev (atom nil)]
    (fn stream [event]
      (when-let [m (:metric event)]
        (let [prev-event (let [prev-event @prev]
                           (reset! prev event)
                           prev-event)]
          (when prev-event
            (let [dt (- (:time event) (:time prev-event))]
              (when-not (zero? dt)
                (let [diff (/ (- m (:metric prev-event)) dt)]
                  (when-not (and remove-neg? (> 0 diff))
                    (call-rescue (assoc event :metric diff) children)))))))))))

(defn ddt
  "Differentiate metrics with respect to time.
  Emits an event for each event received, but with metric equal to
  the difference between the current event and the previous one, divided by the
  difference in their times. Skips events without metrics.

  ```clojure
  (ddt
    (info))
  ```

  If ddt receives {:metric 1 :time 1} and {:metric 10 :time 4}, it will produce
  {:metric (/ 9 3) :time 4}."
  [& children]
  {:action :ddt
   :description {:message "Differentiate metrics with respect to time"}
   :params [false]
   :children children})

(defn ddt-pos
  "Like ddt but do not forward events with negative metrics.
  This can be used for counters which may be reseted to zero for example."
  [& children]
  {:action :ddt-pos
   :description {:message "Differentiate metrics with respect to time"}
   :params [true]
   :children children})

(defn scale*
  [_ factor & children]
  (fn stream [event]
    (call-rescue (update event :metric * factor) children)))

(s/def ::scale (s/cat :factor number?))

(defn scale
  "Multiplies the event :metric field by the factor passed as parameter.

  ```clojure
  (scale 1000
    (info
  ```

  This example will multiply the :metric field for all events by 1000.
  "
  [factor & children]
  (mspec/valid-action? ::scale [factor])
  {:action :scale
   :description {:message (str "Multiples the :metric field by " factor)}
   :params [factor]
   :children children})

(defn split*
  [_ clauses & children]
  (let [clauses (for [index (range (count clauses))]
                  [(nth clauses index) (nth children index)])
        comp-clauses (->> clauses
                          (map (fn [clause]
                                 [(cd/compile-conditions (first clause))
                                  (second clause)])))]
    (fn stream [event]
      (when-let [stream (reduce (fn [state clause]
                                  (if ((first clause) event)
                                    (reduced (second clause))
                                    state))
                                nil
                                comp-clauses)]
        (call-rescue event [stream])))))

(defn split
  "Split by conditions.

  ```clojure
  (split
    [:> :metric 10] (debug)
    [:> :metric 5] (info)
    (error)
  ```

  In this example, all events with :metric > 10 will go into the debug stream,
  all events with :metric > 5 in the info stream, and other events will to the
  default stream which is \"error\".

  The default stream is optional, if not set all events not matching a condition
  will be discarded."
  [& clauses]
  (let [children (atom [])
        ;; can be optimized to not use an atom
        clauses-fn (->> (partition-all 2 clauses)
                        (mapv (fn [clause]
                                (if (second clause)
                                  (do
                                    (swap! children conj (second clause))
                                    (first clause))
                                  ;; add a default fn to the default close
                                  ;; if needed
                                  (do
                                    (swap! children conj (first clause))
                                    [:always-true])))))]
    (mspec/valid-action? (s/coll-of ::condition) clauses-fn)
    {:action :split
     :description {:message (format "Split metrics by the clauses provided as parameter")
                   :params clauses-fn}
     :params [clauses-fn]
     :children @children}))

(defn throttle*
  [_ config & children]
  (let [last-sent (atom [nil nil])]
    (fn stream [event]
      (when (:time event)
        (let [[_ _ event-to-send] (swap! last-sent
                                         (fn [[last-time-sent counter _]]
                                           (cond
                                             ;; window is closed
                                             ;; we send the event and
                                             ;; reset the counter
                                             (or (nil? last-time-sent)
                                                 (>= (:time event)
                                                     (+ last-time-sent (:duration config))))
                                             [(:time event)
                                              1
                                              event]

                                             ;; we reached the threshold
                                             ;; we stop sending
                                             (= counter (:count config))
                                             [last-time-sent
                                              counter
                                              nil]

                                             ;; counter is smaller, we let the event
                                             ;; pass
                                             :else [last-time-sent
                                                    (inc counter)
                                                    event])))]
          (when event-to-send
            (call-rescue event-to-send children)))))))

(s/def ::throttle (s/cat :config (s/keys :req-un [::count ::duration])))

(defn throttle
  "Let N event pass at most every duration seconds.
  Can be used for example to avoid sending to limit the number of alerts
  sent to an external system.

  ```clojure
  (throttle {:count 3 :duration 10}
    (error))
  ```

  In this example, throttle will let 3 events pass at most every 10 seconds.
  Other events, or events with no time, are filtered."
  [config & children]
  (mspec/valid-action? ::throttle [config])
  {:action :throttle
   :description {:message (format "Let %d events pass at most every %d seconds"
                                  (:count config)
                                  (:duration config))}
   :params [(duration->ns config)]
   :children children})

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn moving-event-window*
  [_ config & children]
  (let [window (atom (vec []))]
    (fn stream [event]
      (let [w (swap! window (fn swap [w]
                              (vec (take-last (:size config) (conj w event)))))]
        (call-rescue w children)))))

(s/def ::moving-event-window (s/cat :config (s/keys :req-un [::size])))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn moving-event-window
  "A sliding window of the last few events. Every time an event arrives, calls
  children with a vector of the last n events, from oldest to newest. Ignores
  event times. Example:

  ```clojure
  (moving-event-window {:size 5}
    (coll-mean (info))
  ```"
  [config & children]
  (mspec/valid-action? ::moving-event-window [config])
  {:action :moving-event-window
   :description {:message (format "Build moving event window of size %s"
                                  (:size config))}
   :params [config]
   :children children})

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn ewma-timeless*
  [_ r & children]
  (let [m (atom 0)
        c-existing (- 1 r)
        c-new r]
    (fn stream [event]
      ;; Compute new ewma
      (let [m (when-let [metric-new (:metric event)]
                (swap! m (comp (partial + (* c-new metric-new))
                               (partial * c-existing))))]
        (call-rescue (assoc event :metric m) children)))))

(s/def ::ewma-timeless (s/cat :r number?))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn ewma-timeless
  "Exponential weighted moving average. Constant space and time overhead.
  Passes on each event received, but with metric adjusted to the moving
  average. Does not take the time between events into account. R is the ratio
  between successive events: r=1 means always return the most recent metric;
  r=1/2 means the current event counts for half, the previous event for 1/4,
  the previous event for 1/8, and so on."
  [r & children]
  (mspec/valid-action? ::ewma-timeless [r])
  {:action :ewma-timeless
   :description {:message "Exponential weighted moving average"}
   :params [r]
   :children children})

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn over*
  [_ n & children]
  (fn stream [event]
    (when-let [m (:metric event)]
      (when (< n m)
        (call-rescue event children)))))

(s/def ::over (s/cat :n number?))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn over
  "Passes on events only when their metric is greater than x.

  ```clojure
  (over 10
    (info))
  ```"
  [n & children]
  (mspec/valid-action? ::over [n])
  {:action :over
   :description {:message (format "Keep events with metrics greater than %s" n)}
   :params [n]
   :children children})

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn under*
  [_ n & children]
  (fn stream [event]
    (when-let [m (:metric event)]
      (when (> n m)
        (call-rescue event children)))))

(s/def ::under (s/cat :n number?))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn under
  "Passes on events only when their metric is under than x.

  ```clojure
  (under 10
    (info))
  ```
  "
  [n & children]
  (mspec/valid-action? ::under [n])
  {:action :under
   :description {:message (format "Keep events with metrics under %s" n)}
   :params [n]
   :children children})

(defn changed*
  [_ {:keys [field init]} & children]
  (let [state (atom [init nil])
        nested? (sequential? field)
        get-fn (if nested?
                 get-in
                 get)]
    (fn stream [event]
      (let [[_ event] (swap! state
                             (fn [s]
                               (let [current-val (get-fn event field)]
                                 (if (= (first s)
                                        current-val)
                                   [(first s) nil]
                                   [current-val event]))))]
        (when event
          (call-rescue event children))))))

(s/def ::changed (s/cat :config (s/keys :req-un [::init ::field])))

(defn changed
  "Passes on events only if the `field` passed as parameter differs
  from the previous one.
  The `init` parameter is the default value for the stream.

  ```clojure
  (changed {:field :state :init \"ok\"})
  ```

  For example, this action will let event pass if the :state field vary,
  the initial value being `ok`.

  This stream is useful to get only events making a transition.

  It also supported nested fields:

  ```clojure
  (changed {:field [:nested :field] :init \"ok\"})
  ```"
  [config & children]
  (mspec/valid-action? ::changed [config])
  {:action :changed
   :description {:message (format "Passes on events only if the field %s differs from the previous one (default %s)"
                                  (:field config)
                                  (:init config))}
   :params [config]
   :children children})

(defn project*
  [_ conditions & children]
  (let [conditions-fns (map cd/compile-conditions conditions)
        state (atom {:buffer (reduce #(assoc %1 %2 nil)
                                     {}
                                     (range 0 (count conditions)))
                     :current-time 0})]
    (fn stream [event]
      (let [result (swap! state
                          (fn [{:keys [current-time buffer]}]
                            ;; ffirst compute the current time
                            (let [current-time (cond
                                                 (nil? (:time event))
                                                 current-time

                                                 (> current-time
                                                    (:time event))
                                                 current-time

                                                 :else
                                                 (:time event))]
                              (cond
                                ;; event expired or no time, don't keep it
                                ;; but filter from the current buffer expired events
                                (or (nil? (:time event))
                                    (e/expired? current-time event))
                                {:buffer (->> (map #(when-not (e/expired? current-time (second %))
                                                      %)
                                                   buffer)
                                              (into {}))
                                 :current-time current-time}

                                ;; event not expired, check clauses
                                :else
                                {:buffer (->> (reduce
                                               ;; reduce on buffer
                                               ;; use the key as index
                                               ;; for the condition fn
                                               (fn [state [k v]]
                                                 (let [condition-fn (nth conditions-fns k)
                                                       match? (condition-fn event)]
                                                   (if (and match?
                                                            (or (nil? v)
                                                                (> (:time event) (:time v))))
                                                     ;; if the event match and if the current
                                                     ;; event in the buffer is nil or less recent,
                                                     ;; keep it.
                                                     (conj state [k event])
                                                     ;; else, keep the old one if not expired
                                                     (conj state [k (when (and v
                                                                               (not (e/expired? current-time v)))
                                                                      v)]))))
                                               []
                                               buffer)
                                              (into {}))
                                 :current-time current-time}))))
            events (->> (:buffer result)
                        vals
                        (remove nil?))]
        (when-not (zero? (count events))
          (call-rescue events children))))))

(s/def ::project (s/cat :conditions (s/coll-of ::condition)))

(defn project
  "Takes a list of conditions.
  Like coalesce, project will return the most recent events matching
  the conditions.

  ```clojure
  (project [[:= :service \"enqueues\"]
            [:= :service \"dequeues\"]]
    (coll-quotient
      (with :service \"enqueues per dequeue\"
        (info))))
  ```

  We divide here the latest event for the \"enqueues\" :service by the
  latest event from the \"dequeues\" one.
  "
  [conditions & children]
  (mspec/valid-action? ::project [conditions])
  {:action :project
   :description {:message "return the most recent events matching the conditions"
                 :params (pr-str conditions)}
   :params [conditions]
   :children children})

(defn coll-count*
  [_ & children]
  (fn stream [events]
    ;; send empty event if the list is empty
    (call-rescue (or (math/count-events events)
                     {:metric 0})
                 children)))

(defn coll-count
  "Count the number of events.
  Should receive a list of events from the previous stream.
  The most recent event is used as a base to create the new event, and
  its :metric field is set to the number of events received as input.

  ```clojure
  (fixed-time-window {:duration 60}
    (coll-count
      (debug)))
  ```"
  [& children]
  {:action :coll-count
   :description {:message "Count the number of events"}
   :children children})

(s/def ::sdissoc (s/cat :sdissoc (s/or :single keyword?
                                       :multiple (s/coll-of (s/or
                                                             :keyword keyword?
                                                             :seq (s/coll-of keyword?))))))

(defn sdissoc*
  [_ fields & children]
  (let [to-remove (seq fields)]
    (fn stream [event]
      (call-rescue
       (loop [result event key-list to-remove]
         (if key-list
           (let [key (first key-list)
                 seq-key? (sequential? key)]
             (recur
              (if seq-key?
                (dissoc-in result key)
                (dissoc result key))
              (next key-list)))
           result))
       children))))

(defn sdissoc
  "Remove a key (or a list of keys) from the events.

  ```clojure
  (sdissoc :host (info))

  (sdissoc [:environment :host] (info))

  (sdissoc [:environment [:nested :key]] (info))
  ```"
  [fields & children]
  (mspec/valid-action? ::sdissoc [fields])
  {:action :sdissoc
   :description {:message (format "Remove key(s) %s from events" (str fields))}
   :params [(if (keyword? fields) [fields] fields)]
   :children children})

(defn coll-percentiles*
  [_ points & children]
  (fn stream [events]
    (doseq [event (math/sorted-sample events points)]
      (call-rescue event
                   children))))

(s/def ::coll-percentiles (s/cat :points (s/coll-of number?)))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn coll-percentiles
  "Receives a list of events and selects one
  event from that period for each point. If point is 0, takes the lowest metric
  event.  If point is 1, takes the highest metric event. 0.5 is the median
  event, and so forth. Forwards each of these events to children. The event
  has the point appended the `:quantile` key.
  Useful for extracting histograms and percentiles.

  ```clojure
  (fixed-event-window {:size 10}
    (coll-percentiles [0.5 0.75 0.98 0.99]))
  ```"
  [points & children]
  (mspec/valid-action? ::coll-percentiles [points])
  {:action :coll-percentiles
   :description {:message (format "Computes percentiles for quantiles %s"
                                  (str points))}
   :params [points]
   :children children})


(defn clear-forks
  [state current-time fork-ttl]
  (let [forks (->> (:forks state)
                   (remove #(> (- current-time fork-ttl)
                               (:time (second %))))
                   (into {}))]
    (assoc state :forks forks :last-gc current-time)))

(defn get-fork-and-gc
  [state new-fork fork-name current-time fork-ttl gc-interval]
  (let [current-time (max current-time (:time state current-time))
        state (if (and gc-interval
                       (or (= (:last-gc state) 0)
                           (> current-time (+ (:last-gc state) gc-interval))))
                (clear-forks state current-time fork-ttl)
                state)]
    (if-let [fork (get-in state [:forks fork-name :fork])]
      ;; return the new fork
      (-> (assoc state :returned-fork fork)
          (assoc-in [:forks fork-name :time] current-time))
      (let [new-fork-instance (new-fork)]
        (-> (assoc-in state [:forks fork-name] {:fork new-fork-instance
                                                :time current-time})
            (assoc :returned-fork new-fork-instance))))))

(defn by-get-field-fn
  [field]
  (if (sequential? field)
    #(get-in % field)
    field))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn by-fn
  [{:keys [fields gc-interval fork-ttl]} new-fork]
  (let [f-list (map by-get-field-fn fields)
        f (apply juxt f-list)
        fork-ttl (or fork-ttl 3600)
        state (atom {:last-gc 0})]
    (fn stream [event]
      (let [fork-name (f event)
            fork (:returned-fork (swap! state get-fork-and-gc new-fork fork-name (:time event) fork-ttl gc-interval))]
        (call-rescue event fork)))))

(s/def :by/fields (s/coll-of (s/or :single keyword?
                                   :multiple (s/coll-of keyword?))))
(s/def :by/gc-interval pos-int?)
(s/def :by/fork-ttl pos-int?)

(s/def ::by (s/cat :config (s/keys :req-un [:by/fields]
                                   :opt-un [:by/gc-interval
                                            :by/fork-ttl])))

(defn by
  "Split stream by field
  Every time an event arrives with a new value of field, this action invokes
  its child forms to return a *new*, distinct set of streams for that
  particular value.

  ```clojure
  (by {:fields [:host :service]}
    (fixed-time-window {:duration 60}))
  ```

  This example generates a moving window for each host/service combination.

  You can also pass the `:gc-interval` and `:fork-ttl` keys to the action.
  This will enable garbage collections of children actions, executed every
  `:gc-interval` (in seconds) and which will remove actions which didn't
  receive events since `:fork-ttl` seconds

  ```clojure
  (by {:fields [:host :service [:a :nested-key]
       :gc-interval 3600
       :fork-ttl 1800}
    (fixed-time-window {:duration 60}))
  ```
  "
  [config & children]
  (mspec/valid-action? ::by [config])
  {:action :by
   :description {:message (str "Split streams by field(s) " (:fields config))}
   :params [config]
   :children children})

(defn reinject!*
  [context destination-stream]
  (let [reinject-fn (:reinject context)
        destination-stream (or destination-stream (:source-stream context))]
    (fn stream [event]
      (reinject-fn event destination-stream))))

(s/def ::reinject (s/cat :destination-stream (s/or :keyword keyword?
                                                   :nil nil?)))

(defn reinject!
  "Reinject an event into the streaming system.
  By default, events are reinject into the real time engine. You can reinject
  events to a specific stream by passing the destination stream as parameter.

  ```clojure
  (reinject)
  ```

  This example reinjects events into the real stream engine.

  ```clojure
  (reinject :foo)
  ```

  This example reinjects events into the stream named `:foo`."
  ([]
   (reinject! nil))
  ([destination-stream]
   (mspec/valid-action? ::reinject [destination-stream])
   {:action :reinject!
    :description {:message (format "Reinject events on %s"
                                   (if destination-stream
                                     (str "stream " destination-stream)
                                     "the current stream"))}
    :params [destination-stream]}))

(s/def ::async-queue! (s/cat :queue-name keyword?))

(defn async-queue!*
  [context queue-name & children]
  (if (:test-mode? context)
    (apply sdo* context children)
    (if-let [^Executor executor (get-in context [:outputs queue-name :component])]
      (fn stream [event]
        (.execute executor
                  (fn []
                    (call-rescue event children))))
      (throw (ex/ex-incorrect (format "Async queue %s not found"
                                      queue-name))))))

(defn async-queue!
  "Execute children into the specific async queue.
  The async queue should be defined in the I/O configuration file.

  ```clojure
  (async-queue! :my-queue
    (info))
  ```"
  [queue-name & children]
  (mspec/valid-action? ::async-queue! [queue-name])
  {:action :async-queue!
   :description {:message (format "Execute the children into the queue %s"
                                  queue-name)}
   :params [queue-name]
   :children children})

(defn io*
  [context & children]
  (if (:test-mode? context)
    (fn stream [_] nil)
    (apply sdo* context children)))

(defn io
  "Discard all events in test mode. Else, forward to children.
  You can use this stream to avoid side effects in test mode."
  [& children]
  {:action :io
   :description {:message "Discard all events in test mode"}
   :children children})

(defn tap*
  [context tape-name]
  (if (:test-mode? context)
    (let [tap (:tap context)]
      (fn stream [event]
        (swap! tap
               (fn [tap]
                 (update tap tape-name (fn [v] (if v (conj v event) [event])))))))
    ;; discard in non-tests
    (fn stream [_] nil)))

(s/def ::tap (s/cat :tap-name keyword?))

(defn tap
  "Save events into the tap. Noop outside tests.

  ```clojure
  (where [:= :service \"foo\"]
    (tap :foo)
  ```

  In test mode, all events with `:service` \"foo\" will be saved in a tap
  named `:foo`"
  [tap-name]
  (mspec/valid-action? ::tap [tap-name])
  {:action :tap
   :description {:message (format "Save events into the tap %s" tap-name)}
   :params [tap-name]})

(defn from-json*
  [_ k & children]
  (let [update-fn (if (sequential? k)
                    update-in
                    update)]
    (fn stream [event]
      (call-rescue
       (update-fn event k json/parse-string true)
       children))))

(s/def ::from-json (s/cat :k
                          (s/or :single keyword?
                                :multiple (s/coll-of keyword?))))

(defn from-json
  "Takes a field or a list of fields, and converts the values associated to these
  fields from a json string to edn.

  ```clojure
  (with :my-field \"{\"foo\": \"bar\"}
    (from-json :my-field))
  ```

  In this example, we associate to `:my-field` a json string and then we call
  `from-json` on it. `:my-field` will now contain an edn map built from the json
  data, with keywords as keys.

  This action also supports nested keys by passing an array of keys (for example `(from-json [:nested :key])`)"
  [k & children]
  (mspec/valid-action? ::from-json [k])
  {:action :from-json
   :description {:message "Parse the provided key from a json string to edn"
                 :params (pr-str k)}
   :params [k]
   :children children})

(defn exception->event
  "Build a new event from an Exception and from the event which caused it."
  [^Exception e base-event]
  {:time (:time base-event)
   :name "mirabelle-exception"
   :state "error"
   :metric 1
   :tags ["exception" (.getName (class e))]
   :exception e
   :base-event base-event
   :description (str e "\n\n" (string/join "\n" (.getStackTrace e)))})

(defn exception-stream*
  [_ success-child failure-child]
  (fn stream [event]
    (try
      (success-child event)
      (catch Exception e
        (failure-child (exception->event e event))))))

(defn exception-stream
  "Takes two actions. If an exception is thrown in the first action, an event
  representing this exception is emitted in in the second action.

  ```
  (exception-stream
    (bad-action)
    (error))
  ```

  Here, if `bad-action` throws, an event will be built (using the `exception->event`
  function) and sent to the `error` action (which will log it)."
  [& children]
  (when-not (= 2 (count children))
    (ex/ex-incorrect! "The exception-stream action should take 2 children"
                      {}))
  {:action :exception-stream
   :description {:message "Catches exceptions in the first action and reinject errors into the second one"}
   :children children})

(defn stream
  "Creates a new stream. This action takes a map where the `:name` key, which
  will be the name of the stream, is mandatory."
  [config & children]
  (-> (assoc config :actions (apply sdo children))))

(defn streams
  "Entrypoint for all streams.

  ```clojure
  (streams
    (stream {:name :fobar}
      (info))
    (stream {:name :foo}
      (info)))
  ```"
  [& streams]
  (reduce
   (fn [state stream-config]
     (assoc state (:name stream-config) (dissoc stream-config :name)))
   {}
   streams))

(defn custom
  "Executes a custom action.
  Custom actions are defined in the Mirabelle configuration file.
  The actomn can then be called (by name) using this `custom` action.

  ```clojure
  (custom :my-custom-action [\"parameters\"]
    (info))
  ```"
  [action-name params & children]
  {:action action-name
   :description {:message (str "Use the custom action " action-name)
                 :params (str params)}
   :params (or params [])
   :children children})

(s/def ::to-base64 (s/cat :fields (s/or :keyword keyword?
                                        :seq (s/coll-of keyword?))))

(defn to-base64*
  [_ fields & children]
  (let [update-fn (if (sequential? fields)
                    update-in
                    update)]
    (fn stream [event]
      (call-rescue (update-fn event fields b64/to-base64) children))))

(defn to-base64
  "Convert a field to base64.
  Field value should be string.

  ```clojure
  (sdo
    ;; you can pass one field
    (to-base64 :host)
    ;; or a list to update a nested keys
    (to-base64 [:host :service]))
  ```"
  [field & children]
  (let [fields (if (keyword? field) [field] field)]
    (mspec/valid-action? ::to-base64 [fields])
    {:action :to-base64
     :description {:message (format "Encodes field(s) %s to base64"
                                    field)}
     :params [fields]
     :children children}))

(defn from-base64*
  [_ fields & children]
  (let [update-fn (if (sequential? fields)
                    update-in
                    update)]
    (fn stream [event]
      (call-rescue (update-fn event fields b64/from-base64) children))))

(s/def ::from-base64 (s/cat :fields (s/or :keyword keyword?
                                          :seq (s/coll-of keyword?))))

(defn from-base64
  "Decode a base64 field.

  ```clojure
  (sdo
    ;; you can pass one field
    (from-base64 :host)
    ;; or a list to update a nested keys
    (from-base64 [:host :service]))
  ```"
  [field & children]
  (let [fields (if (keyword? field) [field] field)]
    (mspec/valid-action? ::from-base64 [fields])
    {:action :from-base64
     :description {:message (format "Decodes field(s) %s from base64"
                                    field)}
     :params [fields]
     :children children}))

(defn sformat*
  [_ template target-field fields & children]
  (let [value-fn (fn [event] (reduce #(conj %1
                                            (if (sequential? %2)
                                                (get-in event %2)
                                                (get event %2)))
                                     []
                                     fields))
        assoc-fn (if (sequential? target-field)
                   assoc-in
                   assoc)]
    (fn stream [event]
      (call-rescue
       (assoc-fn event
                 target-field
                 (apply format template (value-fn event)))
       children))))

(s/def ::sformat (s/cat :template string?
                        :target-field (s/or :keyword keyword?
                                            :seq (s/coll-of keyword?))
                        :fields (s/coll-of (s/or :keyword keyword?
                                                 :seq (s/coll-of keyword?)))))

(defn sformat
  "Takes the content of multiple event keys, and use them to build a string value
  and assign it to a given key.

  ```clojure
  (sformat \"%s-foo-%s\" :format-test [:host :service])
  ```

  If the event `{:host \"machine\" :service \"bar\"}` is passed to this action
  the event will become
  `{:host \"machine\" :service \"bar\" :format-test \"machine-foo-bar\"}`.

  It also supports nested keys both for the destination or the fields to extract:

  ```clojure
  (sformat \"%s-foo-%s\" [:nested :key] [[:host :name] :service])
  ```

  More information about availables formatters in the Clojure documentation:
  https://clojuredocs.org/clojure.core/format"
  [template target-field fields & children]
  (mspec/valid-action? ::sformat [template target-field fields])
  {:action :sformat
   :description {:message (format "Set %s to value %s using fields %s"
                                  target-field
                                  template
                                  fields)}
   :params [template target-field fields]
   :children children})

(defn publish!*
  [context channel]
  (let [pubsub (:pubsub context)]
    (fn stream [event]
      (when-not (:test-mode? context)
        (pubsub/publish! pubsub channel event)))))

(s/def ::publish! (s/cat :channel keyword?))

(defn publish!
  "Publish events in the given channel.

  ```clojure
  (publish! :my-channel)
  ```

  Users can then subscribe to channels using the websocket engine."
  [channel]
  (mspec/valid-action? ::publish! [channel])
  {:action :publish!
   :description {:message (str "Publish events into the channel " channel)}
   :params [channel]
   :children []})

(defn coll-top*
  [_ nb-events & children]
  (fn stream [events]
    (call-rescue (math/extremum-n nb-events > events) children)))

(s/def ::coll-top (s/cat :nb-events pos-int?))

(defn coll-top
  "Receives a list of events, returns the top N events with the highest metrics.

  ```clojure
  (fixed-time-window {:duration 60}
    (coll-top 5
      (info)))
  ```"
  [nb-events & children]
  (mspec/valid-action? ::coll-top [nb-events])
  {:action :coll-top
   :description {:message (format "Returns top %d events with the highest metrics"
                                  nb-events)}
   :params [nb-events]
   :children children})

(defn coll-bottom*
  [_ nb-events & children]
  (fn stream [events]
    (call-rescue (math/extremum-n nb-events < events) children)))

(s/def ::coll-bottom (s/cat :nb-events pos-int?))

(defn coll-bottom
  "Receives a list of events, returns the bottom N events with the lowest metrics.

  ```clojure
  (fixed-time-window {:duration 60}
    (coll-bottom 5
      (info)))
  ```"
  [nb-events & children]
  (mspec/valid-action? ::coll-bottom [nb-events])
  {:action :coll-bottom
   :description {:message (format "Returns bottom %d events with the lowest metrics"
                                  nb-events)}
   :params [nb-events]
   :children children})

(defn stable*
  [_ dt field & children]
  (let [state (atom {:last-state nil
                     :buffer []
                     :out nil
                     ;; last flip time
                     :time nil
                     ;; clock
                     :max-time 0})
        nested? (sequential? field)
        get-fn (if nested?
                 get-in
                 get)]
    (fn stream [event]
      (let [event-time (:time event)
            event-state (get-fn event field)]
        (when event-time
          (let [{:keys [out]}
                (swap! state
                       (fn [{:keys [time buffer last-state max-time] :as state}]
                         (if (< event-time max-time)
                           (assoc state :out nil)
                           (cond
                             ;; no time = first event, or else the state
                             ;; was changed
                             (or (not time)
                                 (not= last-state
                                       event-state))
                             {:time event-time
                              :last-state event-state
                              :buffer [event]
                              :out nil
                              :max-time event-time}

                             ;; state is equal, but the dt period is not completed
                             (<= event-time (+ time dt))
                             (-> (update state :buffer conj event)
                                 (assoc :out nil :max-time event-time))

                             ;; state is equal, dt seconds passed
                             ;; the current buffer + the event should be sent
                             (> event-time (+ time dt))
                             {:time time
                              :last-state event-state
                              :buffer []
                              :max-time event-time
                              :out (conj buffer event)}))))]
            (doseq [event out]
              (call-rescue event children))))))))

(s/def ::stable (s/cat :dt pos-int? :field (s/or :keyword keyword?
                                                 :seq (s/coll-of keyword?))))

(defn stable
  "Takes a duration (dt) in second and a field name as parameter.
  Returns events where the value of the field specified as second argument
  is equal to the value of the field for the last event, for at least dt seconds.
  Events can be buffered for dt seconds before being forwarded in order to see
  if they are stable or not.

  Events should arrive in order (old events will be dropped).

  You can use this stream to remove flapping states for example.

  ```clojure
  (stable 10 :state
    (info))
  ```

  In this example, events will be forwarded of the value of the `:state` key
  is the same for at least 10 seconds.

  Support nested fields:

  ```clojure
  (stable 10 [:nested :field]
    (info))
  ```."
  [dt field & children]
  (mspec/valid-action? ::stable [dt field])
  {:action :stable
   :description {:message (format "Returns events where the field %s is stable for more than %s seconds"
                                  field
                                  dt)}
   :params [dt field]
   :children children})

(defn rename-keys*
  [_ replacement & children]
  (fn stream [event]
    (call-rescue
     (reduce
      (fn [event [old new]]
        (let [seq-old (sequential? old)
              val (if seq-old
                    (get-in event old)
                    (get event old))]
          (if val
            (let [event (if seq-old
                          (dissoc-in event old)
                          (dissoc event old))]
              (if (sequential? new)
                (assoc-in event new val)
                (assoc event new val)))
            event)))
      event
      replacement)
     children)))

(s/def ::rename-keys (s/cat :replacement (s/map-of (s/or :keyword keyword?
                                                         :seq (s/coll-of keyword?))
                                                   (s/or :keyword keyword?
                                                         :seq (s/coll-of keyword?)))))

(defn rename-keys
  "Rename events keys.

  ```clojure
  (rename-keys {:host :service
                :environment :env}
  ```

  In this example, the `:host` key will be renamed `:service` and the
  `:environment` key is renamed `:env`.

  You can also pass a list of keys as source or destination to rename nested attributes:

  ```clojure
  (rename-keys {[:attribute source] [:attribute: destination]
                :state [:attribute :state]
                [:attribute :host] :host})
  ```

  Existing values will be overrided.
"
  [replacement & children]
  (mspec/valid-action? ::rename-keys [replacement])
  {:action :rename-keys
   :description {:message "Rename events keys"
                 :params (pr-str replacement)}
   :params [replacement]
   :children children})

(defn keep-keys*
  [_ keys-to-keep & children]
  (let [keyseq (seq keys-to-keep)]
    (fn stream [event]
      (call-rescue
       (select-keys-nested event keyseq)
       children))))

(s/def ::keep-keys (s/cat :keys-to-keep (s/coll-of (s/or :keyword keyword?
                                                         :seq (s/coll-of keyword?)))))

(defn keep-keys
  "Keep only the specified keys for events.

  ```clojure
  (keep-keys [:host :metric :time :environment :description]
    (info))
  ```

  Also works with nested keys:

  ```clojure
  (keep-keys [:host :metric :time [:a :nested-key]]
    (info))
  ```
"
  [keys-to-keep & children]
  (mspec/valid-action? ::keep-keys [keys-to-keep])
  {:action :keep-keys
   :description {:message "Keep only the specified keys from events"
                 :params (pr-str keys-to-keep)}
   :params [keys-to-keep]
   :children children})

(defmethod aero/reader 'mirabelle/var
  [opts _ value]
  (when-not (keyword? value)
    (throw (ex-info "The argument of #mirabelle/var should be a keyword"
                    {:variable value})))
  (let [variables (:variables opts)]
    (get variables value)))

(s/def :include/variables (s/map-of keyword? any?))
(s/def :include/profile keyword?)
(s/def :include/config (s/keys :opt-un [:include/variables :include/profile]))
(s/def ::include (s/cat :path ::spec/ne-string
                        :config :include/config))

(defn get-env-profile
  []
  (some-> (System/getenv "PROFILE")
          keyword))

(defn include
  "Include an configuration file by path into the configuration. The file will be read
  using the aero (https://github.com/juxt/aero/) library.
  The `config` variable supports these optional options:

  - `:profile`: the aero profile to use. By default, Mirabelle will read (and convert
  to a Clojure keyword) the PROFILE environment variable during compilation.
  You can override this value by setting `:profile`.
  - `:variables`: variables to pass to the configuration file.
  You can use the `#mirabelle/var` reader in order to define variables in your
  EDN file.

  This allows you to use the same configuration snippet (eventually templated) from
  multiple streams (or multiple parts of the same stream)

  ```clojure
  (includes\"/etc/mirabelle/includes/my-actions.clj {:profile :dev
                                                     :variables {:foo \"bar\"})
  ```"
  [path config]
  (mspec/valid-action? ::include [path config])
  (binding [*ns* (find-ns 'mirabelle.action)]
    (let [profile (or (:profile config)
                      (get-env-profile))]
      (eval (aero/read-config
             path
             (cond-> {}
               profile (assoc :profile profile)
               (:variables config) (assoc :variables (:variables config))))))))

(defn ratio-state-update
  [state event state-fn cond1 cond2]
  (cond-> state
    (cond1 event) (state-fn event :first-cond)
    (cond2 event) (state-fn event :last-cond)))

(def keyword->aggr-fn
  {:max (fn [config]
          (fn [state event]
            (if state
              (if (> (:metric state) (:metric event))
                state
                event)
              event)))
   :ssort (fn [config]
            (fn [state event]
              (if state
                (conj state event)
                [event])))
   :rate (fn [config]
           (fn [state event]
             (if state
               (-> (update state :count inc)
                   (assoc :event (e/most-recent-event event (:event state))))
               {:count 1
                :event event})))
   :min (fn [config]
          (fn [state event]
            (if state
              (if (< (:metric state) (:metric event))
                state
                event)
              event)))
   :mean (fn [config]
           (fn [state event]
             (if state
               (-> (update state :sum + (:metric event 0))
                   (update :count inc)
                   (assoc :event (e/most-recent-event event (:event state))))
               {:sum (:metric event 0)
                :count 1
                :event event})))
   :fixed-time-window (fn [config]
                        (fn [state event]
                          (if state
                            (conj state event)
                            [event])))
   :ratio (fn [{:keys [conditions metric]}]
            (let [[cond1 cond2] (map cd/compile-conditions conditions)
                  state-fn (if metric
                             (fn [state event k]
                               (update state k + (:metric event 0)))
                             (fn [state event k]
                               (update state k inc)))]
              (fn [state event]
                (if state
                  (-> (ratio-state-update state event state-fn cond1 cond2)
                      (assoc :event (e/most-recent-event event (:event state))))
                  (ratio-state-update {:first-cond 0 :last-cond 0 :event event}
                                      event
                                      state-fn
                                      cond1
                                      cond2)))))
   :+ (fn [config]
        (fn [state event]
          (if state
            (if (> (:time event) (:time state))
              (update event :metric + (:metric state))
              (update state :metric + (:metric event)))
            event)))})

(def keyword->aggr-finalizer-fn
  {:ssort (fn [config windows]
            (sort-by
             (if (:nested? config)
               (fn [event] (get-in event (:field config)))
               (:field config))
             (flatten windows)))
   :ratio (fn [config windows]
            (map (fn [window]
                   (assoc (:event window)
                          :metric
                          (if (zero? (:last-cond window))
                            0
                            (/ (:first-cond window) (:last-cond window))))) windows))
   :rate (fn [config windows]
           (map (fn [window]
                  (assoc (:event window)
                         :metric
                         (/ (:count window)
                            (time/ns->s (:duration config)))))
                windows))
   :mean (fn [_ windows]
           (map (fn [window]
                  (assoc (:event window) :metric (/ (:sum window) (:count window))))
                windows))})

(defn default-aggr-finalizer
  [_ event]
  event)

(defn get-window
  [event start-time duration]
  (let [window (/ (- (:time event) start-time) duration)]
    (if (>= window 0)
      (long window)
      (dec (long window)))))

(defn aggregation*
  [_ {:keys [duration] :as config} & children]
  (let [accepted-delay (:delay config 0)
        finalizer-fn (get keyword->aggr-finalizer-fn
                          (:aggr-fn config)
                          default-aggr-finalizer)
        aggr-fn ((get keyword->aggr-fn (:aggr-fn config)) config)
        state (atom {:start-time nil
                     :current-window nil
                     :current-time nil
                     :windows {}
                     :to-send nil})]
    (when-not aggr-fn
      (ex/ex-fault! (format "Invalid aggregation function %s" aggr-fn)
                    {:aggr-fn aggr-fn}))
    (fn stream [event]
      (let [s (swap! state
                     (fn [{:keys [start-time current-window current-time] :as state}]
                       (cond
                         ;; No start time, initialize everything
                         (nil? start-time)
                         ;; I'm sure the state can be simplified (eg deduct
                         ;; start time from current time/window or stuff like that)
                         ;; but at least this implementation works
                         (assoc state
                                :start-time (:time event)
                                :current-time (:time event)
                                :current-window (get-window event (:time event) duration)
                                :to-send nil
                                :windows {(get-window event (:time event) duration)
                                          (aggr-fn nil event)})

                         ;; before current window
                         (< (get-window event start-time duration)
                            current-window)
                         (if (< (:time event)
                                (- current-time
                                   accepted-delay))
                           ;; too old, just drop it
                           (assoc state :to-send nil)
                           ;; in toleration
                           (-> (update-in state
                                          [:windows (get-window event start-time duration)]
                                          aggr-fn
                                          event)
                               (assoc :to-send nil)))
                         ;; within or above windows
                         :else
                         (let [window (get-window event start-time duration)
                               windows (:windows state)
                               current-time (max (:current-time state)
                                                 (:time event))
                               windows-to-send (->> (keys windows)
                                                    (filter #(>= (- current-time accepted-delay)
                                                                 ;; time of the end of the old window
                                                                 (+ start-time (* (inc %) duration)))))]
                           (-> (update state :windows
                                       #(apply dissoc % windows-to-send))
                               (update-in [:windows window]
                                          aggr-fn
                                          event)
                               (assoc :current-window window
                                      :current-time current-time
                                      :to-send (vals (select-keys windows windows-to-send))))))))]
        (when-let [windows (:to-send s)]
          (doseq [w (finalizer-fn config windows)]
            (call-rescue w
                         children)))))))

(s/def ::sum (s/cat :config (s/keys :req-un [::duration]
                                    :opt-un [::delay])))

(s/def ::top (s/cat :config (s/keys :req-un [::duration]
                                    :opt-un [::delay])))

(s/def ::bottom (s/cat :config (s/keys :req-un [::duration]
                                       :opt-un [::delay])))

(s/def ::mean (s/cat :config (s/keys :req-un [::duration]
                                     :opt-un [::delay])))

(defn sum
  "Sum the events field from the last dt seconds.

  ```clojure
  (sum {:duration 10}
    (info))
  ```

  You can pass a `:delay` key to the configuration in order to tolerate late
  events. In that case, events from previous windows will be flushed after this
  delay:

  ```clojure
  (sum {:duration 10 :delay 5}
    (info))
  ```"
  [config & children]
  (mspec/valid-action? ::sum [config])
  {:action :sum
   :description {:message (format "Sum the events field from the last %s seconds"
                                  (:duration config))}
   :params [(duration->ns (assoc config :aggr-fn :+))]
   :children children})

(defn top
  "Get the max event from the last dt seconds.

  ```clojure
  (top {:duration 10}
    (info))
  ```

  You can pass a `:delay` key to the configuration in order to tolerate late
  events. In that case, events from previous windows will be flushed after this
  delay:

  ```clojure
  (top {:duration 10 :delay 5}
    (info))
  ```"
  [config & children]
  (mspec/valid-action? ::top [config])
  {:action :top
   :description {:message (format "Get the max event from the last %s seconds"
                                  (:duration config))}
   :params [(duration->ns (assoc config :aggr-fn :max))]
   :children children})

(defn bottom
  "Get the min event from the last dt seconds.

  ```clojure
  (bottom {:duration 10}
    (info))
  ```

  You can pass a `:delay` key to the configuration in order to tolerate late
  events. In that case, events from previous windows will be flushed after this
  delay:

  ```clojure
  (bottom {:duration 10 :delay 5}
    (info))
  ```"
  [config & children]
  (mspec/valid-action? ::bottom [config])
  {:action :bottom
   :description {:message (format "Get the min event from the last %s seconds"
                                  (:duration config))}
   :params [(duration->ns (assoc config :aggr-fn :min))]
   :children children})

(defn mean
  "Get the mean of event metrics from the last dt seconds.

  ```clojure
  (mean {:duration 10}
    (info))
  ```

  You can pass a `:delay` key to the configuration in order to tolerate late
  events. In that case, events from previous windows will be flushed after this
  delay:

  ```clojure
  (mean {:duration 10 :delay 5}
    (info))
  ```"
  [config & children]
  (mspec/valid-action? ::mean [config])
  {:action :mean
   :description {:message (format "Get the min of events from the last %s seconds"
                                  (:duration config))}
   :params [(duration->ns (assoc config :aggr-fn :mean))]
   :children children})

(s/def ::fixed-time-window (s/cat :config
                                  (s/keys :req-un [::duration]
                                          :opt-un [::delay])))

(defn fixed-time-window
  "A fixed window over the event stream in time. Emits vectors of events, such
  that each vector has events from a distinct n-second interval. Windows do
  *not* overlap; each event appears at most once in the output stream.

  ```clojure
  (fixed-time-window {:duration 60}
    (coll-max
      (info)))
  ```

  You can pass a `:delay` key to the configuration in order to tolerate late
  events. In that case, previous windows will be flushed after this
  delay:

  ```clojure
  (fixed-time-window {:duration 60 :delay 30}
    (coll-max
      (info)))
  ```"
  [config & children]
  (mspec/valid-action? ::fixed-time-window [config])
  {:action :fixed-time-window
   :description {:message (format "Build %d seconds fixed time windows"
                                  (:duration config))}
   :params [(duration->ns (assoc config :aggr-fn :fixed-time-window))]
   :children children})

(defn moving-time-window*
  [_ {:keys [duration]} & children]
  (let [state (atom {:cutoff 0
                     :buffer []
                     :send? true})]
    (fn stream [event]
      (let [result (swap!
                    state
                    (fn [{:keys [cutoff buffer]}]
                                        ; Compute minimum allowed time
                      (let [cutoff (max cutoff (- (get event :time 0) duration))
                            send? (or (nil? (:time event))
                                      (< cutoff (:time event)))
                            buffer (if send?
                                        ; This event belongs in the buffer,
                                        ; and our cutoff may have changed.
                                     (vec (filter
                                           (fn [e] (or (nil? (:time e))
                                                       (< cutoff (:time e))))
                                           (conj buffer event)))
                                     buffer)]
                        {:cutoff cutoff
                         :buffer buffer
                         :send? send?})))]
        (when (:send? result)
          (call-rescue (:buffer result) children))))))

(s/def ::moving-time-window (s/cat :config (s/keys :req-un [::duration])))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn moving-time-window
  "A sliding window of all events with times within the last n seconds. Uses
  the maximum event time as the present-time horizon. Every time a new event
  arrives within the window, emits a vector of events in the window to
  children.

  Events without times accrue in the current window."
  [config & children]
  (mspec/valid-action? ::moving-time-window [config])
  {:action :moving-time-window
   :description {:message (format "Build sliding windows of %d seconds"
                                  (:duration config))}
   :params [(duration->ns config)]
   :children children})

(s/def ::ssort (s/cat :config (s/keys :req-un [::duration ::field]
                                      :opt-un [::delay])))

(defn ssort
  "Streaming sort.
  Takes a configuration containing a `:duration` and a `:field` key.
  The action will buffer events during `:duration` seconds and then
  send the events downstream one by one, sorted by `:field`.

  ```clojure
  (ssort {:duration 10 :field :time}
    (info))
  ```

  This example will sort events based on the :time field.

  For example, if it get as input:

  ```clojure
  {:time 1} {:time 10} {:time 4} {:time 9} {:time 13} {:time 31}
  ```

  Info will receive these events:

  ```clojure
  {:time 1} {:time 4} {:time 9} {:time 10} {:time 13}
  ```

  You can add a `:delay` key to the action configuration in order to tolerate
  late events:

  ```clojure
  (ssort {:duration 10 :field :time :delay 10}
    (info))
  ```

  In this example, events from previous windows will be sent with a delay of
  10 seconds.

  `ssort` supports sorting on a nested field (example `:field [:nested :field]`)"
  [config & children]
  (mspec/valid-action? ::ssort [config])
  {:action :ssort
   :description {:message (format "Sort events during %d seconds based on the field %s"
                                  (:duration config)
                                  (:field config))}
   :params [(duration->ns
             (assoc config
                    :aggr-fn :ssort
                    :nested? (sequential? (:field config))))]
   :children children})

(defn coll-increase*
  [_ & children]
  (fn [[event & events]]
    (when (and event events)
      (let [{:keys [most-recent-event oldest-event]}
            (reduce
             (fn [{:keys [most-recent-event oldest-event] :as state} event]
               (cond

                 (< (:time event) (:time oldest-event))
                 (assoc state :oldest-event event)

                 (> (:time event) (:time most-recent-event))
                 (assoc state :most-recent-event event)

                 :else
                 state))
             {:most-recent-event event
              :oldest-event event}
             events)
            new-metric (- (:metric most-recent-event)
                          (:metric oldest-event))]
        ;; check if the counter was resetted
        (when (> new-metric 0)
          (call-rescue (assoc most-recent-event :metric new-metric) children))))))

(defn coll-increase
  "Receives a list of events which should represent an always-increasing counter
  and returns the latest event with its :metric field set to the value of the
  increase between the oldest and the latest event.

  If it receives for example:

  ```clojure
  [{:time 1 :metric 10} {:time 9 :metric 20} {:time 20 :metric 30}}
  ```

  It will produces (30-10 = 20):

  ```clojure
  {:time 20 :metric 20}
  ```

  Events produced with a negative metric (which can happen if the counter is resetted) are not send downstream."
  [& children]
  {:action :coll-increase
   :description {:message "Takes a list of events and computes the increase of the :metric field"}
   :children children})

(defn scondition*
  [_ {:keys [condition]} & children]
  (let [result (atom {})]
    (fn [event]
      (if (condition event @result)
        (do (reset! result event)
            (call-rescue event children))
        (call-rescue @result children)))))

(defn smax
  "Send downstream the event with the biggest :metric every time it receives an event

  ```clojure
  (smax
    (info))
  ```

  If the events `{:time 1 :metric 10} {:time 2 :metric 3} {:time 3 :metric 11}`
  are injected, `info` will receive:

  ```
  {:time 1 :metric 10} {:time 1 :metric 10} {:time 3 :metric 11}
  ```
  "
  [& children]
  {:action :smax
   :description {:message "Send downstream the event with the biggest :metric every time it receives an event"}
   :params [{:condition (fn [event result]
                          (or (not (:metric result))
                              (> (:metric event) (:metric result))))}]
   :children children})

(defn smin
  "Send downstream the event with the lowest :metric every time it receives an event

  ```clojure
  (smin
    (info))
  ```

  If the events `{:time 1 :metric 10} {:time 2 :metric 3} {:time 3 :metric 11}`
  are injected, `info` will receive:

  ```clojure
  {:time 1 :metric 10} {:time 2 :metric 3} {:time 3 :metric 11}
  ```
  "
  [& children]
  {:action :smin
   :description {:message "Send downstream the event with the lowest :metric every time it receives an event"}
   :params [{:condition (fn [event result]
                          (or (not (:metric result))
                              (< (:metric event) (:metric result))))}]
   :children children})

(defn extract*
  [_ k & children]
  (let [get-fn (if (sequential? k)
                 get-in
                 get)]
    (fn [event]
      (when-let [result (get-fn event k)]
        (call-rescue result children)))))

(s/def ::extract (s/cat :k (s/or :keyword keyword?
                                 :seq (s/coll-of keyword?))))

(defn extract
  "Takes a key as parameter and send downstream the content of this key.

  ```clojure
  (extract :base-event
    (info))
  ```

  If `extract` receives in this example `{:time 1 :base-event {:time 1 :service \"foo\" :host \"bar\"}`, `info` will receive the content of `:base-time`.

  It also supports nested keys:


  ```clojure
  (extract [:foo :bar]
    (info))
  ```"
  [k & children]
  (mspec/valid-action? ::extract [k])
  {:action :extract
   :description {:message (format "Extract the key %s from the event and send its value downstream" k)}
   :params [k]
   :children children})

(s/def ::rate (s/cat :config (s/keys :req-un [::duration]
                                     :opt-un [::delay])))

(defn rate
  "Computes the rate of received events (by counting them) and emits the result at a periodic interval"
  [config & children]
  (mspec/valid-action? ::rate [config])
  {:action :rate
   :description {:message (format "Computes the rate of received events (by counting them) and emits it every %d seconds" (::duration config))}
   :params [(duration->ns (assoc config :aggr-fn :rate))]
   :children children})

(defn percentiles*
  [_ {:keys [duration
             percentiles
             delay
             highest-trackable-value
             nb-significant-digits
             lowest-discernible-value]
      :or {nb-significant-digits 3}}
   & children]
  (let [^Recorder recorder
        (cond
          lowest-discernible-value (Recorder. lowest-discernible-value
                                              highest-trackable-value
                                              nb-significant-digits)

          highest-trackable-value (Recorder. ^Long highest-trackable-value
                                             ^Integer nb-significant-digits)

          :else (Recorder. ^Integer nb-significant-digits))
        state (atom {:last-flush 0
                     :emit? nil})
        delay (or delay 0)]
    (fn stream [event]
      (let [state (swap! state (fn [{:keys [last-flush]}]
                                 (cond
                                   (= 0 last-flush)
                                   {:last-flush (:time event) :emit? false}

                                   (> (:time event)
                                      (+ last-flush duration))
                                   {:last-flush (:time event) :emit? true}

                                   (>= (:time event)
                                       (- last-flush delay))
                                   {:last-flush last-flush :emit? false}

                                   :else
                                   {:last-flush last-flush :emit? false :discard true})))]
        (when-not (:discard state)
          (if (:emit? state)
            (let [^Histogram histogram (.getIntervalHistogram recorder)]
              (.recordValue recorder (:metric event))
              (doseq [percentile percentiles]
                (call-rescue (-> (assoc event
                                        :metric (.getValueAtPercentile histogram
                                                                      (double (* 100 percentile))))
                                 (assoc-in [:attributes :quantile] (str percentile)))
                             children)))
            (.recordValue recorder (:metric event))))))))

(s/def ::highest-trackable-value pos-int?)
(s/def ::nb-significant-digits pos-int?)
(s/def ::lowest-discernible-value number?)

(s/def :percentiles/percentiles (s/coll-of number?))

(s/def ::percentiles (s/cat :config (s/keys :req-un [::duration
                                                     :percentiles/percentiles]
                                            :opt-un [::delay
                                                     ::nb-significant-digits
                                                     ::highest-trackable-value
                                                     ::lowest-discernible-value])))


(defn percentiles
  "Computes quantiles based on a stream of events. Results are flushed periodically downstreal based on the action configuration

   ```clojure
   (a/percentiles {:percentiles [0.5 0.75 0.99]
                   :duration 10})
   ```

  `:percentiles` contains a list of quantiles to compute, `:duration` is the duration before sending the result downstream, `:nb-significant-digits` is the precision for the computation (default to 3).

  After the end of the period, the action will generate one event for each percentile that is computed, with the key `:quantile` set to the percentile value. In this example, it would generate 3 events with `:quantile` equal to `0.5`, `0.75` or `0.99`.

  The action also supports optinal options: a `:delay` option to tolerate late events (in that case the current window will be flushed `:delay` seconds after its end), and `:highest-trackabe-value` and `:lowest-discernible-value` to configure the histogram computation.

  See hdrhistogram documentation (http://hdrhistogram.org/) for more information about these settings."
  [config & children]
  (mspec/valid-action? ::percentiles [config])
  {:action :percentiles
   :description {:message (format "Computes the quantiles %s" (:percentiles config))}
   :params [(duration->ns config)]
   :children children})

(s/def ::to-string (s/coll-of (s/or :keyword keyword?
                                    :coll (s/coll-of keyword?))))

(defn to-string*
  [_ keys & children]
  (fn stream [event]
    (call-rescue
     (reduce
      (fn [event k]
        (if (keyword? k)
          (update event k str)
          (update-in event k str)))
      event
      keys)
     children)))

(defn to-string
  "Converts values associated to keys to string. nil values are converted to an empty string.
  The parameter is a list of keys. It supports updating nested keys by passing a list of keywords.

  ```clojure
  (to-string [:service :state]
    (info))
  ```

  ```clojure
  (to-string [[:attributes :name] :quantile]
    (info))
  ```"
  [keys & children]
  (mspec/valid-action? ::to-string keys)
  {:action :to-string
   :description {:message (format "Convert to string the values associated to keys %s" keys)}
   :params [keys]
   :children children})

(s/def :ratio/metric boolean?)
(s/def :ratio/conditions (s/tuple ::condition ::condition))

(s/def ::ratio (s/cat :config (s/keys :req-un [::duration
                                               :ratio/conditions]
                                      :opt-un [::delay
                                               :ratio/metric])))

(defn ratio
  "Computes the ratio for 2 conditions:

  ```clojure
  (ratio {:duration 30
          :conditions [[:= :state \"error\"] [:true]]}
    (info))
  ```

  In this example, the action will count the number of events matching each conditions (:state = \"error, and :true matching all events).

  Every 30 seconds, an event will be send downstream with the :metric field set to the ratio between the count of events for the second condition and the count for the first one.
  For example, if the action received 30 events in total during the interval, and with 5 with :state = \"error\", :metric will be equal to 5/30.

  The latest event is used as a base to build the event that is sent downstream.

  It also supports a :metric option. If set to true, the action will sum the :metric value of events matching conditions instead of counting them.

  ```clojure
  (ratio {:duration 30
          :conditions [[:= :state \"error\"] [:true]]
          :metric true}
    (info))
  ```

  You can pass a `:delay` key to the configuration in order to tolerate late
  events. In that case, events from previous windows will be flushed after this
  delay."
  [config & children]
  (mspec/valid-action? ::ratio [config])
  {:action :ratio
   :description {:message (format "TODO %s"
                                  (:duration config))}
   :params [(duration->ns (assoc config :aggr-fn :ratio))]
   :children children})

(defn iterate-on*
  [_ {:keys [source destination]} & children]
  (fn stream [event]
    (let [base (get event source)]
      (when (sequential? base)
        (doseq [b base]
          (call-rescue
           (-> (assoc event destination b)
               (dissoc source))
           children))))))


(s/def :iterate-on/source (s/or :keyword keyword? :vec (s/coll-of keyword?)))
(s/def :iterate-on/destination (s/or :keyword keyword? :vec (s/coll-of keyword?)))

(s/def ::iterate-on (s/cat :config (s/keys :req-un [:iterate-on/source
                                                    :iterate-on/destination])))

(defn iterate-on
  [config & children]
  (mspec/valid-action? ::iterate-on [config])
  {:action :iterate-on
   :description {:message (format"Extract the list from source %s and create new events for each element, with the element mapped to the destination %s" (:source config) (:destination config))}
   :params [config]
   :children children})

(def action->fn
  {:above-dt cond-dt*
   :sum aggregation*
   :async-queue! async-queue!*
   :below-dt cond-dt*
   :between-dt cond-dt*
   :bottom aggregation*
   :changed changed*
   :coalesce coalesce*
   :coll-bottom coll-bottom*
   :coll-count coll-count*
   :coll-increase coll-increase*
   :coll-max coll-max*
   :coll-mean coll-mean*
   :coll-min coll-min*
   :coll-percentiles coll-percentiles*
   :coll-quotient coll-quotient*
   :coll-rate coll-rate*
   :coll-sort coll-sort*
   :coll-sum coll-sum*
   :coll-top coll-top*
   :coll-where coll-where*
   :cond-dt cond-dt*
   :debug debug*
   :default default*
   :decrement decrement*
   :ddt ddt*
   :ddt-pos ddt*
   :info info*
   :iterate-on iterate-on*
   :error error*
   :extract extract*
   :ewma-timeless ewma-timeless*
   :exception-stream exception-stream*
   :expired expired*
   :fixed-event-window fixed-event-window*
   :fixed-time-window aggregation*
   :from-base64 from-base64*
   :increment increment*
   :io io*
   :from-json from-json*
   :keep-keys keep-keys*
   :mean aggregation*
   :moving-event-window moving-event-window*
   :moving-time-window moving-time-window*
   :not-expired not-expired*
   :outside-dt cond-dt*
   :over over*
   :percentiles percentiles*
   :project project*
   :publish! publish!*
   :output! output!*
   :rate aggregation*
   :ratio aggregation*
   :reinject! reinject!*
   :rename-keys rename-keys*
   :scale scale*
   :sdissoc sdissoc*
   :sdo sdo*
   :sflatten sflatten*
   :sformat sformat*
   :smax scondition*
   :smin scondition*
   :split split*
   :ssort aggregation*
   :stable stable*
   :tag tag*
   :tagged-all tagged-all*
   :tap tap*
   :test-action test-action*
   :throttle throttle*
   :to-base64 to-base64*
   :to-string to-string*
   :top aggregation*
   :under under*
   :untag untag*
   :where where*
   :with with*})

(defn gen-doc
  []
  (spit "/tmp/mirabelle-doc"
        (->> (assoc action->fn :include nil)
             keys
             (map name)
             sort
             (reduce (fn [s v] (str s
                                    (format "\n- [%s](https://mirabelle.mcorbin.fr/generated-doc/mirabelle.action.html#var-%s)" v v))) ""))))
