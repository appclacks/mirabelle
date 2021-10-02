(ns mirabelle.action
  (:require [aero.core :as aero]
            [cheshire.core :as json]
            [clojure.set :as set]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [corbihttp.log :as log]
            [exoscale.ex :as ex]
            [mirabelle.action.condition :as cd]
            [mirabelle.b64 :as b64]
            [mirabelle.db.queue :as queue]
            [mirabelle.event :as e]
            [mirabelle.index :as index]
            [mirabelle.io :as io]
            [mirabelle.math :as math]
            [mirabelle.pubsub :as pubsub]
            [mirabelle.spec :as spec])
  (:import java.util.concurrent.Executor))

(s/def ::size pos-int?)
(s/def ::duration pos-int?)
(s/def ::threshold number?)
(s/def ::high number?)
(s/def ::low number?)
(s/def ::field keyword?)
(s/def ::init any?)
(s/def ::fields (s/coll-of keyword?))
(s/def ::count pos-int?)

(defn call-rescue
  [event children]
  (doseq [child children]
    (child event)))

(defn discard-fn
  [e]
  (some #(= "discard" %) (:tags e)))

(defn keep-non-discarded-events
  "Takes an event or a list of events. Returns an event (or a list of events
  depending of the input) with all events tagged \"discard\" filtered.
  Returns nil if all events are filtered."
  [events]
  (if (sequential? events)
    (let [result (remove discard-fn
                         events)]
      (when-not (empty? result)
        result))
    (when-not (discard-fn events)
      events)))

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
  (spec/valid-action? ::where [conditions])
  {:action :where
   :params [conditions]
   :children children})

(defn increment*
  [_ & children]
  (fn stream [event]
    (call-rescue (update event :metric inc)
                 children)))

(defn increment
  "Increment the event :metric field.

  ```
  (increment
    (index [:host]))
  ```
  "
  [& children]
  {:action :increment
   :children children})

(defn decrement*
  [_ & children]
  (fn stream [event]
    (call-rescue (update event :metric dec)
                 children)))

(defn decrement
  "Decrement the event :metric field.

  ```
  (decrement
    (index [:host]))
  ```
  "
  [& children]
  {:action :decrement
   :children children})

(defn log-action
  "Generic logger"
  [level]
  (fn stream [event]
    (when-let [event (keep-non-discarded-events event)]
      (condp = level
        :debug (log/debug {} (json/generate-string event))
        :info (log/info {} (json/generate-string event))
        :error (log/error {} (json/generate-string event))))))

(defn debug*
  [_]
  (log-action :debug))

(defn debug
  "Print the event in the logs using the debug level

  ```clojure
  (increment
    (debug))
  ```"
  []
  {:action :debug})

(defn info*
  [_]
  (log-action :info))

(defn info
  "Print the event in the logs using the info level

  ```clojure
  (increment
    (info))
  ```"
  []
  {:action :info})

(defn error*
  [_]
  (log-action :error))

(defn error
  "Print the event in the logs using the error level

  ```clojure
  (increment
    (debug))
  ```"
  []
  {:action :error})

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
  (spec/valid-action? ::fixed-event-window [config])
  {:action :fixed-event-window
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

  Get the event the biggest metric on windows of 10 events"
  [& children]
  {:action :coll-max
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

  Get the event the smallest metric on windows of 10 events"
  [& children]
  {:action :coll-min
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
  (spec/valid-action? ::above-dt [config])
  {:action :above-dt
   :params [[:> :metric (:threshold config)] (:duration config)]
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
  (spec/valid-action? ::below-dt [config])
  {:action :below-dt
   :params [[:< :metric (:threshold config)] (:duration config)]
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
  (spec/valid-action? ::between-dt [config])
  {:action :between-dt
   :params [[:and
             [:> :metric (:low config)]
             [:< :metric (:high config)]]
            (:duration config)]
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
  (spec/valid-action? ::outside-dt [config])
  {:action :outside-dt
   :params [[:or
             [:< :metric (:low config)]
             [:> :metric (:high config)]]
            (:duration config)]
   :children children})

(s/def ::critical-dt (s/cat :config (s/keys :req-un [::duration])))

(defn critical-dt
  "Takes a time period in seconds `duration`.
  If all events received during at least the period `duration` have `:state` critical,
  new critical events received after the `duration` period will be passed on until
  an invalid event arrives.

  ```clojure
  (critical-dt {:duration 10}
    (debug))
  ```

  In this example, if the events `:state` are \"critical\" for more than 10 seconds, events are passed downstream.
  "
  [config & children]
  (spec/valid-action? ::critical-dt [config])
  {:action :critical-dt
   :params [[:= :state "critical"]
            (:duration config)]
   :children children})

(defn critical*
  [_ & children]
  (fn stream [event]
    (when (e/critical? event)
      (call-rescue event children))))

(defn critical
  "Keep all events in state critical.

  ```clojure
  (critical
    (error))
  ```

  In this example, all events with `:state` \"critical\" will be logged.
  "
  [& children]
  {:action :critical
   :children children})

(defn warning*
  [_ & children]
  (fn stream [event]
    (when (e/warning? event)
      (call-rescue event children))))

(defn warning
  "Keep all events in state warning.

  ```clojure
  (warning
    (warning))
  ```

  In this example, all events with `:state` \"warning\" will be logged.
  "
  [& children]
  {:action :warning
   :children children})

(defn default*
  [_ field value & children]
  (fn stream [event]
    (if-not (get event field)
      (call-rescue (assoc event field value) children)
      (call-rescue event children))))

(s/def ::default (s/cat :field spec/not-null :value any?))

(defn default
  "Set a default value for an event

  ```clojure
  (default :state \"ok\"
    (info))
  ```

  In this example, all events where `:state` is not set will be updated with
  `:state` to \"ok\"."
  [field value & children]
  (spec/valid-action? ::default [field value])
  {:action :default
   :params [field value]
   :children children})

(defn push-io!*
  [context io-name]
  ;; discard io in test mode
  (if (:test-mode? context)
    (fn stream [_] nil)
    (if-let [io-component (get-in context [:io io-name :component])]
      (fn stream [event]
        (when-let [events (keep-non-discarded-events event)]
          (io/inject! io-component (e/sequential-events events))))
      (throw (ex/ex-incorrect (format "IO %s not found"
                                      io-name))))))

(s/def ::push-io! (s/cat :io-name keyword?))

(defn push-io!
  "Push events to an external system.

  I/O are defined in a dedicated file. If you create a new I/O named `:influxdb`
  for example, you can use push-io! to push all events into this I/O:

  ```clojure
  (push-io! :influxdb)
  ```

  I/O are automatically discarded in test mode."
  [io-name]
  (spec/valid-action? ::push-io! [io-name])
  {:action :push-io!
   :params [io-name]})

(defn coalesce*
  [_ {:keys [duration fields]} & children]
  (let [state (atom {:buffer {}
                     :current-time 0
                     :last-tick nil
                     :window nil})
        key-fn #(vals (select-keys % fields))]
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
  "
  [config & children]
  (spec/valid-action? ::coalesce [config])
  {:action :coalesce
   :children children
   :params [config]})

(defn with*
  [_ fields & children]
  (fn stream [event]
    (call-rescue (merge event fields) children)))

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
  to \"critical\" for events."
  [& args]
  (cond
    (map? (first args))
    {:action :with
     :children (rest args)
     :params [(first args)]}

    :else
    (let [[k v & children] args]
      (when (or (not k) (not v))
        (throw (ex/ex-info (format "Invalid parameters for with: %s %s" k v)
                           {})))
      {:action :with
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
  (spec/valid-action? ::tag [tags])
  {:action :tag
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
  (untag \"foo\" index)
  ```

  This example removes the tag \"foo\" from events.

  ```clojure
  (untag [\"foo\" \"bar\"] index)
  ```

  This example removes the tags \"foo\" and \"bar\" from events"
  [tags & children]
  (spec/valid-action? ::untag [tags])
  {:action :untag
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
  (spec/valid-action? ::tagged-all [tags])
  {:action :tagged-all
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
  "Differentiate metrics with respect to time. Takes an optional number
  followed by child streams.
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
   :params [false]
   :children children})

(defn ddt-pos
  "Like ddt but do not forward events with negative metrics.
  This can be used for counters which may be reseted to zero for example."
  [& children]
  {:action :ddt-pos
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
  (spec/valid-action? ::scale [factor])
  {:action :scale
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

  ```
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
    (spec/valid-action? (s/coll-of ::condition) clauses-fn)
    {:action :split
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
  (spec/valid-action? ::throttle [config])
  {:action :throttle
   :params [config]
   :children children})

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn fixed-time-window*
  [_ {:keys [duration]} & children]
  (let [state (atom {:start-time nil
                     :buffer []
                     :windows nil})]
    (fn stream [event]
      (let [s (swap! state
                     (fn [{:keys [start-time buffer] :as state}]
                       (cond
                                        ; No time
                         (nil? (:time event))
                         (-> (update state :buffer conj event)
                             (assoc :windows nil))

                                        ; No start time
                         (nil? start-time)
                         (assoc state :start-time (:time event)
                                :buffer [event]
                                :windows nil)

                                        ; Too old
                         (< (:time event) start-time)
                         (assoc state :windows nil)

                                        ; Within window
                         (< (:time event) (+ start-time duration))
                         (-> (update state :buffer conj event)
                             (assoc :windows nil))

                                        ; Above window
                         :else
                         (let [delta (- (:time event) start-time)
                               dstart (- delta (mod delta duration))
                               empties (dec (/ dstart duration))
                               ;; do we really need empty windows in
                               ;; mirabelle ? Should we keep this Riemann
                               ;; behavior ?
                               windows (conj (repeat empties []) buffer)]
                           (-> (update state :start-time + dstart)
                               (assoc :buffer [event]
                                      :windows windows))))))]
        (when-let [windows (:windows s)]
          (doseq [w windows]
            (call-rescue w children)))))))

(s/def ::fixed-time-window (s/cat :config (s/keys :req-un [::duration])))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn fixed-time-window
  "A fixed window over the event stream in time. Emits vectors of events, such
  that each vector has events from a distinct n-second interval. Windows do
  *not* overlap; each event appears at most once in the output stream. Once an
  event is emitted, all events *older or equal* to that emitted event are
  silently dropped.

  Events without times accrue in the current window.

  ```clojure
  (fixed-time-window {:duration 60}
    (coll-max
      (info)))
  ```
  "
  [config & children]
  (spec/valid-action? ::fixed-time-window [config])
  {:action :fixed-time-window
   :params [config]
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
  (spec/valid-action? ::moving-event-window [config])
  {:action :moving-event-window
   :params [config]
   :children children})

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn ewma-timeless*
  [_ r & children]
  (let [m (atom 0)
        c-existing (- 1 r)
        c-new r]
    (fn stream [event]
                                        ; Compute new ewma
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
  (spec/valid-action? ::ewma-timeless [r])
  {:action :ewma-timeless
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
  (spec/valid-action? ::over [n])
  {:action :over
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
  (spec/valid-action? ::under [n])
  {:action :under
   :params [n]
   :children children})

(defn changed*
  [_ {:keys [field init]} & children]
  (let [state (atom [init nil])]
    (fn stream [event]
      (let [[_ event] (swap! state
                             (fn [s]
                               (let [current-val (get event field)]
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

  This stream is useful to get only events making a transition."
  [config & children]
  (spec/valid-action? ::changed [config])
  {:action :changed
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
  (spec/valid-action? ::project [conditions])
  {:action :project
   :params [conditions]
   :children children})

(defn index*
  [context labels]
  (let [i (:index context)
        channel (index/channel (:source-stream context))
        default-channel (index/channel :default)
        pubsub (:pubsub context)]
    (fn stream [event]
      (when-let [t (:time event)]
        (index/new-time? i t))
      (when-not (:test-mode? context)
        (pubsub/publish! pubsub channel event)
        (when (:default context)
          (pubsub/publish! pubsub default-channel event)))
      (index/insert i event labels))))

(s/def ::index (s/cat :labels (s/coll-of keyword?)))

(defn index
  "Insert events into the index.
  Events are indexed using the keys passed as parameter.

  ```clojure
  (index [:host :service])
  ```

  This example will index events by host and services."
  [labels]
  (spec/valid-action? ::index [labels])
  {:action :index
   :params [labels]})

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
   :children children})

(s/def ::sdissoc (s/cat :sdissoc (s/or :single keyword?
                                       :multiple (s/coll-of keyword?))))

(defn sdissoc*
  [_ fields & children]
  (fn stream [event]
    (call-rescue (apply dissoc event fields)
                 children)))

(defn sdissoc
  "Remove a key (or a list of keys) from the events/

  ```clojure
  (sdissoc :host (info))

  (sdissoc [:environment :host] (info))
  ```"
  [fields & children]
  (spec/valid-action? ::sdissoc [fields])
  {:action :sdissoc
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
  (spec/valid-action? ::coll-percentiles [points])
  {:action :coll-percentiles
   :params [points]
   :children children})

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn by-fn
  [fields new-fork]
  (let [fields (flatten [fields])
        f (if (= 1 (count fields))
            (first fields)
            (apply juxt fields))
        table (atom {})]
    (fn stream [event]
      (let [fork-name (f event)
            fork (if-let [fork (@table fork-name)]
                   fork
                   ((swap! table assoc fork-name (new-fork)) fork-name))]
        (call-rescue event fork)))))

(s/def ::by (s/cat :fields (s/coll-of keyword?)))

(defn by
  "Split stream by field
  Every time an event arrives with a new value of field, this action invokes
  its child forms to return a *new*, distinct set of streams for that
  particular value.

  ```clojure
  (by [:host :service]
    (fixed-time-window {:duration 60}))
  ```

  This example generates a moving window for each host/service combination."
  [fields & children]
  (spec/valid-action? ::by [fields])
  {:action :by
   :params [fields]
   :children children})

(defn disk-queue!*
  [context]
  (if (:test-mode? context)
    (fn stream [_] nil)
    (let [queue (:queue context)]
      (fn stream [events]
        (when-let [events (keep-non-discarded-events events)]
          (queue/write! queue events))))))

(defn disk-queue!
  "Write events into the on-disk queue."
  []
  {:action :disk-queue!
   :params []})

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
   (spec/valid-action? ::reinject [destination-stream])
   {:action :reinject!
    :params [destination-stream]}))

(s/def ::async-queue! (s/cat :queue-name keyword?))

(defn async-queue!*
  [context queue-name & children]
  (if (:test-mode? context)
    (apply sdo* context children)
    (if-let [^Executor executor (get-in context [:io queue-name :component])]
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
  (spec/valid-action? ::async-queue! [queue-name])
  {:action :async-queue!
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
  (spec/valid-action? ::tap [tap-name])
  {:action :tap
   :params [tap-name]})

(defn json-fields*
  [_ fields & children]
  (fn stream [event]
    (call-rescue
     (reduce (fn [event field]
               (if (get event field)
                 (update event field json/parse-string true)
                 event))
             event
             fields)
     children)))

(s/def ::json-fields (s/cat :fields
                            (s/or :single keyword?
                                  :multiple (s/coll-of keyword?))))

(defn json-fields
  "Takes a field or a list of fields, and converts the values associated to these
  fields from json to edn.

  ```clojure
  (with :my-field \"{\"foo\": \"bar\"}
    (json-fields [:my-field]))
  ```

  In this example, we associate to `:my-field` a json string and then we call
  `json-fields` on it. `:my-field` will now contain an edn map built from the json
  data, with keywords as keys.
  "
  [fields & children]
  (spec/valid-action? ::json-fields [fields])
  {:action :tap
   :params [(if (keyword? fields) [fields] fields)]
   :children children})

(defn exception->event
  "Build a new event from an Exception and from the event which caused it."
  [^Exception e base-event]
  {:time (:time base-event)
   :service "mirabelle-exception"
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
   :params (or params [])
   :children children})

(defn reaper*
  [context interval destination-stream]
  (let [index (:index context)
        clock (atom [0 false])
        reinject-fn (:reinject context)
        destination-stream (or destination-stream (:source-stream context))]
    (fn stream [event]
      (when (:time event)
        (let [[_ expire?] (swap! clock (fn [[previous-tick _ :as s]]
                                         (if (>= (:time event)
                                                 (+ interval previous-tick))
                                           [(:time event) true]
                                           s)))]
          (when expire?
            (doseq [event (index/expire index)]
              (reinject-fn event destination-stream))))))))

(s/def ::reaper (s/cat :interval pos-int?
                       :destination-stream (s/or :keyword keyword?
                                                 :nil nil?)))

(defn reaper
  "Everytime this action receives an event, it will expires events from the
  index (every dt seconds) and reinject them into a stream
  (default to the current stream if not specified).

  ```clojure
  (reaper 5)
  ```

  ```clojure
  (reaper 5 :custom-stream)
  ```"
  ([interval] (reaper interval nil))
  ([interval destination-stream]
   (spec/valid-action? ::reaper [interval destination-stream])
   {:action :reaper
    :params [interval destination-stream]
    :children []}))

(s/def ::to-base64 (s/cat :fields (s/coll-of keyword?)))

(defn to-base64*
  [_ fields & children]
  (fn stream [event]
    (call-rescue (reduce #(update %1 %2 b64/to-base64) event fields) children)))

(defn to-base64
  "Convert a field or multiple fields to base64.
  Fields values should be string.

  ```clojure
  (sdo
    ;; you can pass one field
    (to-base64 :host)
    ;; or a list of fields
    (to-base64 [:host :service]))
  ```
  "
  [field & children]
  (let [fields (if (keyword? field) [field] field)]
    (spec/valid-action? ::to-base64 [fields])
    {:action :to-base64
     :params [fields]
     :children children}))

(defn from-base64*
  [_ fields & children]
  (fn stream [event]
    (call-rescue (reduce #(update %1 %2 b64/from-base64) event fields) children)))

(s/def ::from-base64 (s/cat :fields (s/coll-of keyword?)))

(defn from-base64
  "Convert a field or multiple fields from base64 to string.
  Fields values should be string.

  ```clojure
  (sdo
    ;; you can pass one field
    (from-base64 :host)
    ;; or a list of fields
    (from-base64 [:host :service]))
  ```
  "
  [field & children]
  (let [fields (if (keyword? field) [field] field)]
    (spec/valid-action? ::from-base64 [fields])
    {:action :from-base64
     :params [fields]
     :children children}))

(defn sformat*
  [_ template target-field fields & children]
  (let [value-fn (fn [event] (reduce #(conj %1 (get event %2)) [] fields))]
    (fn stream [event]
      (call-rescue
       (assoc event
              target-field
              (apply format template (value-fn event)))
       children))))

(s/def ::sformat (s/cat :template string?
                        :target-field keyword?
                        :fields (s/coll-of keyword?)))

(defn sformat
  "Takes the content of multiple event keys, and use them to build a string value
  and assign it to a given key.

  ```clojure
  (sformat \"%s-foo-%s\" :format-test [:host :service])
  ```

  If the event `{:host \"machine\" :service \"bar\"}` is passed to this action
  the event will become
  `{:host \"machine\" :service \"bar\" :format-test \"machine-foo-bar\"}`.

  More information about availables formatters in the Clojure documentation:
  https://clojuredocs.org/clojure.core/format"
  [template target-field fields & children]
  (spec/valid-action? ::sformat [template target-field fields])
  {:action :sformat
   :params [template target-field fields]
   :children children})

(defn publish!*
  [context channel]
  (let [pubsub (:pubsub context)]
    (fn stream [event]
      (when-not (:test-mode? context)
        (when-let [event (keep-non-discarded-events event)]
          (pubsub/publish! pubsub channel event))))))

(s/def ::publish! (s/cat :channel keyword?))

(defn publish!
  "Publish events in the given channel.

  ```clojure
  (publish! :my-channel)
  ```

  Users can then subscribe to channels using the websocket engine."
  [channel]
  (spec/valid-action? ::publish! [channel])
  {:action :publish!
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
    (coll-top
      (info)))
  ```"
  [nb-events & children]
  (spec/valid-action? ::coll-top [nb-events])
  {:action :coll-top
   :params [nb-events]
   :children children})

(defn coll-bottom*
  [_ nb-events & children]
  (fn stream [events]
    (call-rescue (math/extremum-n nb-events < events) children)))

(s/def ::coll-bottom (s/cat :nb-events pos-int?))

(defn coll-bottom
  "Receives a list of events, returns the top N events with the lowest metrics.

  ```clojure
  (fixed-time-window {:duration 60}
    (coll-bottom
      (info)))
  ```"
  [nb-events & children]
  (spec/valid-action? ::coll-bottom [nb-events])
  {:action :coll-bottom
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
                     :max-time 0})]
    (fn stream [event]
      (let [event-time (:time event)
            event-state (get event field)]
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

(s/def ::stable (s/cat :dt pos-int? :field keyword?))

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
  is the same for at least 10 seconds"
  [dt field & children]
  (spec/valid-action? ::stable [dt field])
  {:action :stable
   :params [dt field]
   :children children})

(defn rename-keys*
  [_ replacement & children]
  (fn stream [event]
    (call-rescue (set/rename-keys event replacement) children)))

(s/def ::rename-keys (s/cat :replacement (s/map-of keyword? keyword?)))

(defn rename-keys
  "Rename events keys.

  ```clojure
  (rename-keys {:host :service
                :environment :env}
  ```

  In this example, the `:host` key will be renamed `:service` and the
  `:environment` key is renamed `:env`.
  Existing values will be overrided."
  [replacement & children]
  (spec/valid-action? ::rename-keys [replacement])
  {:action :rename-keys
   :params [replacement]
   :children children})

(defn keep-keys*
  [_ keys-to-keep & children]
  (fn stream [event]
    (call-rescue (select-keys event keys-to-keep) children)))

(s/def ::keep-keys (s/cat :keys-to-keep (s/coll-of keyword?)))

(defn keep-keys
  "Keep only the specified keys for events.

  ```clojure
  (keep-keys [:host :metric :time :environment :description]
    (info))
  ```"
  [keys-to-keep & children]
  (spec/valid-action? ::keep-keys [keys-to-keep])
  {:action :keep-keys
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
  (spec/valid-action? ::include [path config])
  (binding [*ns* (find-ns 'mirabelle.action)]
    (let [profile (or (:profile config)
                      (get-env-profile))]
      (eval (aero/read-config
             path
             (cond-> {}
               profile (assoc :profile profile)
               (:variables config) (assoc :variables (:variables config))))))))

(def action->fn
  {:above-dt cond-dt*
   :async-queue! async-queue!*
   :below-dt cond-dt*
   :between-dt cond-dt*
   :changed changed*
   :coalesce coalesce*
   :coll-bottom coll-bottom*
   :coll-count coll-count*
   :coll-max coll-max*
   :coll-mean coll-mean*
   :coll-min coll-min*
   :coll-percentiles coll-percentiles*
   :coll-quotient coll-quotient*
   :coll-rate coll-rate*
   :coll-sum coll-sum*
   :coll-top coll-top*
   :critical critical*
   :critical-dt cond-dt*
   :debug debug*
   :default default*
   :decrement decrement*
   :ddt ddt*
   :ddt-pos ddt*
   :disk-queue! disk-queue!*
   :info info*
   :error error*
   :ewma-timeless ewma-timeless*
   :exception-stream exception-stream*
   :expired expired*
   :fixed-event-window fixed-event-window*
   :fixed-time-window fixed-time-window*
   :from-base64 from-base64*
   :increment increment*
   :index index*
   :io io*
   :json-fields json-fields*
   :keep-keys keep-keys*
   :moving-event-window moving-event-window*
   :not-expired not-expired*
   :outside-dt cond-dt*
   :over over*
   :project project*
   :publish! publish!*
   :push-io! push-io!*
   :reaper reaper*
   :reinject! reinject!*
   :rename-keys rename-keys*
   :scale scale*
   :sdissoc sdissoc*
   :sdo sdo*
   :sflatten sflatten*
   :sformat sformat*
   :split split*
   :stable stable*
   :tag tag*
   :tagged-all tagged-all*
   :tap tap*
   :test-action test-action*
   :throttle throttle*
   :to-base64 to-base64*
   :under under*
   :untag untag*
   :warning warning*
   :where where*
   :with with*})

(comment (->> (assoc action->fn :include nil) keys (map name) sort (reduce (fn [s v] (str s "\n- " v)) "")))
