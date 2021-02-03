(ns mirabelle.action
  (:require [clojure.spec.alpha :as s]
            [corbihttp.log :as log]
            [exoscale.ex :as ex]
            [mirabelle.event :as e]
            [mirabelle.io :as io]
            [mirabelle.math :as math]
            [mirabelle.spec :as spec]))

(defn call-rescue
  [event children]
  (doseq [child children]
    (child event)))

(def condition->fn
  "Map containing the functions associated to the where options"
  {:pos? pos?
   :neg? neg?
   :zero? zero?
   :> >
   :>= >=
   :< <
   :<= <=
   := =
   :always-true (constantly true)
   :contains (fn [field value]
               (some #(= value %) field))
   :absent (fn [field value] (not (some #( = value %) field)))
   :regex #(re-matches %2 %1)
   :nil? nil?
   :not-nil? (comp not nil?)
   :not= not=})

(defn valid-condition?
  [condition]
  (and
   (sequential? condition)
   (cond
     (or (= :or (first condition))
         (= :and (first condition)))
     (every? identity (map #(valid-condition? %) (rest condition)))

     :else
     (and ((-> condition->fn keys set)
           (first condition))
          (keyword? (second condition))))))

(defn compile-condition
  [[condition field & args]]
  (let [condition-fn (get condition->fn condition)
        regex? (= :regex condition)
        args (if regex?
               [(-> (first args) re-pattern)]
               args)]
    (fn [event] (apply condition-fn
                       (get event field)
                       args))))

(defn compile-conditions
  "Takes a condition and returns a function which can be applied to an
  event to check if the condition is valid for this event"
  [conditions]
  (let [compile-conditions-fn
        (fn [cd] (reduce
                  (fn [state condition]
                    (conj state (compile-condition condition)))
                  []
                  cd))]
    (cond
      (= :or (first conditions))
      (let [cond-fns (compile-conditions-fn (rest conditions))]
        (fn [event] (some identity (map #(% event) cond-fns))))

      (= :and (first conditions))
      (let [cond-fns (compile-conditions-fn (rest conditions))]
        (fn [event] (every? identity (map #(% event) cond-fns))))

      :else
      (let [cond-fn (compile-condition conditions)]
        (fn [event] (cond-fn event))))))

(defn where*
  [_ conditions & children]
  (let [condition-fn (compile-conditions conditions)]
    (fn [event]
      (when (condition-fn event)
        (call-rescue event children)))))

(s/def ::condition valid-condition?)
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
  (spec/valid? ::where [conditions])
  {:action :where
   :params [conditions]
   :children children})

(defn increment*
  [_ & children]
  (fn [event]
    (call-rescue (update event :metric inc)
                 children)))

(defn increment
  "Decrement the event :metric field."
  [& children]
  {:action :increment
   :children children})

(defn decrement*
  [_ & children]
  (fn [event]
    (call-rescue (update event :metric dec)
                 children)))

(defn decrement
  "Decrement the event :metric field."
  [& children]
  {:action :decrement
   :children children})

(defn debug*
  [_ & children]
  (fn [event]
    (log/debug {} (pr-str event))
    (call-rescue event children)))

(defn debug
  "Print the event in the logs using the debug level

  ```clojure
  (increment
    (debug))
  ```"
  [& children]
  {:action :debug
   :children children})

(defn info*
  [_ & children]
  (fn [event]
    (log/info {} (pr-str event))
    (call-rescue event children)))

(defn info
  "Print the event in the logs using the info level

  ```clojure
  (increment
    (debug))
  ```"
  [& children]
  {:action :info
   :children children})

(defn error*
  [_ & children]
  (fn [event]
    (log/error {} (pr-str event))
    (call-rescue event children)))

(defn error
  "Print the event in the logs using the error level

  ```clojure
  (increment
    (debug))
  ```"
  [& children]
  {:action :error
   :children children})

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn fixed-event-window*
  [_ size & children]
  (let [window (atom [])]
    (fn [event]
      (let [events (swap! window (fn [events]
                                   (let [events (conj events event)]
                                     (if (< size (count events))
                                       [event]
                                       events))))]
        (when (= size (count events))
          (call-rescue events children))))))

(s/def ::fixed-event-window (s/cat :size pos-int?))

(defn fixed-event-window
  "Returns a fixed-sized window of events

  ```clojure
  (fixed-event-window 5
    (debug))
  ```

  This example will return a vector events partitioned 5 by 5."
  [size & children]
  (spec/valid? ::fixed-event-window [size])
  {:action :fixed-event-window
   :params [size]
   :children children})

(defn list-mean*
  [_ & children]
  (fn [events]
    (call-rescue (math/mean events) children)))

(defn list-mean
  "Computes the events mean (on metric).
  Should receive a list of events from the previous stream.
  The most recent event is used as a base to create the new event

  ```clojure
  (fixed-event-window 10
    (list-mean
      (debug)))
  ```

  Computes the mean on windows of 10 events"
  [& children]
  {:action :list-mean
   :children children})

(defn list-max*
  [_ & children]
  (fn [events]
    (call-rescue (math/max-event events) children)))

(defn list-max
  "Returns the event with the biggest metric.
  Should receive a list of events from the previous stream.

  ```clojure
  (fixed-event-window 10
    (list-max
      (debug)))
  ```

  Get the event the biggest metric on windows of 10 events"
  [& children]
  {:action :list-max
   :children children})

(defn list-min*
  [_ & children]
  (fn [events]
    (call-rescue (math/min-event events) children)))

(defn list-min
  "Returns the event with the smallest metric.
  Should receive a list of events from the previous stream.

  ```clojure
  (fixed-event-window 10
    (list-min
      (debug)))
  ```

  Get the event the smallest metric on windows of 10 events"
  [& children]
  {:action :list-min
   :children children})

(defn test-action*
  [_ state]
  (fn [event]
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
  (fn [event]
    (call-rescue event children)))

(defn sdo
  "Send events to children

  ```clojure
  (sdo
    (increment)
    (decrement))
  ```"
  [& children]
  {:action :sdo
   :children children})

(defn expired*
  "Keep expired events."
  [_ & children]
  (let [time-state (atom 0)]
    (fn [event]
      (let [current-time (swap! time-state (fn [old-time]
                                             (max old-time (:time event 0))))]
        (when (e/expired? current-time event)
          (call-rescue event children))))))

(defn expired
  "Keep expired events

  ```clojure
  (expired
    (increment))
  ```"
  [& children]
  {:action :expired
   :children children})

(defn not-expired*
  "Keep non-expired events."
  [_ & children]
  (let [time-state (atom 0)]
    (fn [event]
      (let [current-time (swap! time-state (fn [old-time]
                                             (max old-time (:time event 0))))]
        (when (not (e/expired? current-time event))
          (call-rescue event children))))))

(defn not-expired
  "Keep non-expired events

  ```clojure
  (not-expired
    (increment))
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
  (let [condition-fn (compile-conditions conditions)
        last-changed-state (atom {:ok false
                                  :time nil})]
    (fn [event]
      (let [{ok :ok changed-state-time :time} @last-changed-state
            event-time (:time event)
            valid-event (condition-fn event)]
        (when event-time ;; filter events with no time
          (swap! last-changed-state (fn [state]
                        (cond
                          (and valid-event (and (not ok)
                                                (or (not changed-state-time)
                                                    (> event-time changed-state-time))))
                          ;; event is validating the condition
                          ;; last event is not ok, has no time or is too old
                          ;; => last-changed-state is now ok with a new time
                          {:ok true :time event-time}
                          (and (not valid-event) (and ok
                                                      (or (not changed-state-time)
                                                          (> event-time changed-state-time))))
                          ;; event is not validating the condition
                          ;; last event is ok, has no time or is too old
                          ;; => last-changed-state is now ko with a new time
                          {:ok false :time event-time}
                          ;; default value, return the state
                          :default state)))
          (when (and valid-event
                      ;; we already had an ok event
                     ok
                     ;; check is current time > first ok event + dt
                     (> event-time (+ changed-state-time dt)))
            (call-rescue event children)))))))

(s/def ::above-dt (s/cat :threshold pos-int? :dt pos-int?))

(defn above-dt
   "Takes a number `threshold` and a time period in seconds `dt`.
  If the condition `the event metric is > to the threshold` is valid for all events
  received during at least the period `dt`, valid events received after the `dt`
  period will be passed on until an invalid event arrives.
  `:metric` should not be nil (it will produce exceptions)."
  [threshold dt & children]
  (spec/valid? ::above-dt [threshold dt])
  {:action :above-dt
   :params [[:> :metric threshold] dt]
   :children children})

(s/def ::below-dt (s/cat :threshold pos-int? :dt pos-int?))

(defn below-dt
    "Takes a number `threshold` and a time period in seconds `dt`.
  If the condition `the event metric is < to the threshold` is valid for all
  events received during at least the period `dt`, valid events received after
  the `dt` period will be passed on until an invalid event arrives.
  `:metric` should not be nil (it will produce exceptions)."
  [threshold dt & children]
  (spec/valid? ::below-dt [threshold dt])
  {:action :below-dt
   :params [[:< :metric threshold] dt]
   :children children})

(s/def ::between-dt (s/cat :low pos-int? :high pos-int? :dt pos-int?))

(defn between-dt
    "Takes two numbers, `low` and `high`, and a time period in seconds, `dt`.
  If the condition `the event metric is > low and < high` is valid for all events
  received during at least the period `dt`, valid events received after the `dt`
  period will be passed on until an invalid event arrives.
  `:metric` should not be nil (it will produce exceptions)."
  [low high dt & children]
  (spec/valid? ::between-dt [low high dt])
  {:action :between-dt
   :params [[:and
             [:> :metric low]
             [:< :metric high]]
            dt]
   :children children})

(s/def ::outside-dt (s/cat :low pos-int? :high pos-int? :dt pos-int?))

(defn outside-dt
    "Takes two numbers, `low` and `high`, and a time period in seconds, `dt`.
  If the condition `the event metric is < low or > high` is valid for all events
  received during at least the period `dt`, valid events received after the `dt`
  period will be passed on until an invalid event arrives.
  `:metric` should not be nil (it will produce exceptions)."
  [low high dt & children]
  (spec/valid? ::outside-dt [low high dt])
  {:action :outside-dt
   :params [[:or
             [:< :metric low]
             [:> :metric high]]
            dt]
   :children children})

(s/def ::critical-dt (s/cat :dt pos-int?))

(defn critical-dt
  "Takes a time period in seconds `dt`.
  If all events received during at least the period `dt` have `:state` critical,
  new critical events received after the `dt` period will be passed on until
  an invalid event arrives."
  [dt & children]
  (spec/valid? ::critical-dt [dt])
  {:action :critical-dt
   :params [[:= :state "critical"]
            dt]
   :children children})

(defn critical*
  [_ & children]
  (fn [event]
    (when (e/critical? event)
      (call-rescue event children))))

(defn critical
  "Keep all events in state critical"
  [& children]
  {:action :critical
   :children children})

(defn default*
  [_ field value & children]
  (fn [event]
    (if-not (get event field)
      (call-rescue (assoc event field value) children)
      (call-rescue event children))))

(s/def ::default (s/cat :field spec/not-null :value any?))

(defn default
  "Set a default value for an event"
  [field value & children]
  (spec/valid? ::default [field value])
  {:action :default
   :params [field value]
   :children children})

(defn push-io!*
  [context io-name]
  (let [io-component (get-in context [:io io-name])]
    (fn [event]
      (io/inject! io-component event))))

(s/def ::push-io! (s/cat :io-name keyword?))

(defn push-io!
  "Push events to an external system"
  [io-name]
  (spec/valid? ::push-io! [io-name])
  {:action :push-io!
   :params [io-name]})

(defn coalesce*
  [_ dt fields & children]
  (let [state (atom {:buffer {}
                     :current-time 0
                     :last-tick nil
                     :window nil})
        key-fn #(vals (select-keys % fields))]
    ;; the implementation can probably be optimized ?
    (fn [event]
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
                           (< current-time (+ last-tick dt))
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

(s/def ::coalesce (s/cat :dt pos-int? :fields (s/coll-of keyword?)))

(defn coalesce
  "Returns a list of the latest non-expired events (by `fields`) every dt seconds
  (at best).

  ```clojure
  (coalesce 10 [:host \"service\"]
    (debug)
  ```

  In this example, the latest event for each host/service combination will be
  kept and forwarded downstream. Expired events will be removed from the list.
  "
  [dt fields & children]
  (spec/valid? ::coalesce [dt fields])
  {:action :coalesce
   :children children
   :params [dt fields]})

(defn with*
  [_ fields & children]
  (fn [event]
    (call-rescue (merge event fields) children)))

(s/def ::with (s/cat :field keyword? :value any?))

(defn with
  "Set an event field to the given value.

  ```clojure
  (with :state \"critical\"
    (debug))
  ```

  A map can also be provided:

  ```
  ```clojure
  (with {:host nil :state \"critical\"}
    (debug))
  ```

  This example set the field :state to critical for events."
  [& args]
  (cond
;    (spec/valid? ::with [field value])

    (map? (first args))
    {:action :with
     :children (rest args)
     :params [(first args)]}

    :else
    (let [[k v & children] args]
      (if (or (not k) (not v))
        (throw (ex/ex-info "Invalid parameters for with: %s %s" k v)))
      {:action :with
       :children children
       :params [{k v}]})))

(defn list-rate*
  [_  & children]
  (fn [events]
    (call-rescue (math/rate events) children)))

(defn list-rate
  "Computes the rate on a list of events.
  Should receive a list of events from the previous stream.
  The latest event is used as a base to build the new event.

  ```clojure
  (fixed-event-window 3
    (list-rate
      (debug)))
  ```

  If this example receives the events:

  {:metric 1 :time 1} {:metric 2 :time 2} {:metric 1 :time 3}

  The stream will return {:metric 2 :time 3}

  Indeed, (1+2+1)/2 = 3 (we divide by 2 because we have 2 seconds between the
  min and max events time).
  "
  [& children]
  {:action :list-rate
   :children children})

(defn sflatten*
  [_ & children]
  (fn [events]
    (doseq [e events]
      (call-rescue e children))))

(defn sflatten
  "Streaming flatten. Calls children with each event in events.
  Events should be a sequence.

  This stream can be used to \"flat\" a sequence of events (emitted
  by a time window stream for example)."
  [& children]
  {:action :sflatten
   :children children})

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn tag*
  [_ tags & children]
  (let [tags (flatten [tags])]
    (fn [event]
      (call-rescue
       (assoc event :tags (distinct (concat tags (:tags event))))
       children))))

(s/def ::tag (s/cat :tags (s/or :single string?
                                :multiple (s/coll-of string?))))

(defn tag
  "Adds a new tag, or set of tags, to events which flow through.

  (tag \"foo\" index)
  (tag [\"foo\" \"bar\"] index)"
  [tags & children]
  (spec/valid? ::tag [tags])
  {:action :tag
   :params [tags]
   :children children})

(defn untag*
  [_ tags & children]
  (let [tags (set (flatten [tags]))
        blacklist #(not (tags %))]
    (fn [event]
      (call-rescue (update event :tags #(filter blacklist %))
                   children))))

(s/def ::untag (s/cat :tags (s/or :single string?
                                  :multiple (s/coll-of string?))))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn untag
  "Removes a tag, or set of tags, from events which flow through.

  (untag \"foo\" index)
  (untag [\"foo\" \"bar\"] index)"
  [tags & children]
  (spec/valid? ::untag [tags])
  {:action :untag
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
  difference in their times. Skips events without metrics."
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
  (fn [event]
    (call-rescue (update event :metric * factor) children)))

(s/def ::scale (s/cat :factor number?))

(defn scale
  "Multiplies the event :metric field by :factor"
  [factor & children]
  (spec/valid? ::scale [factor])
  {:action :scale
   :params [factor]
   :children children})

(defn split*
  [_ clauses & children]
  (println "clauses:" clauses  " children:"children)
  (let [clauses (for [index (range (count clauses))]
                  [(nth clauses index) (nth children index)])
        comp-clauses (->> clauses
                          (map (fn [clause]
                                 [(compile-conditions (first clause))
                                  (second clause)])))]
    (fn [event]
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
    (critical)
  ```

  In this example, all events with :metric > 10 will go into the debug stream,
  all events with :metric > 5 in the info stream, and other events will to the
  default stream which is \"critical\".

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
    {:action :split
     :params [clauses-fn]
     :children @children}))

(defn throttle*
  [_ dt & children]
  (let [last-sent (atom [nil nil])]
    (fn [event]
      (when (:time event)
        (let [[_ event-to-send] (swap! last-sent
                                       (fn [[last-time-sent _]]
                                         (if (or (nil? last-time-sent)
                                                 (>= (:time event)
                                                     (+ last-time-sent dt)))
                                           [(:time event) event]

                                           [last-time-sent nil])))]
          (when event-to-send
            (call-rescue event-to-send children)))))))

(s/def ::throttle (s/cat :dt number?))

(defn throttle
  "Let one event pass at most every dt seconds.
  Can be used for example to avoid sending to limit the number of alerts
  sent to an external system.

  ```clojure
  (throttle 10
    (alert))
  ```

  In this example, throttle will let one event pass at most every 10 seconds.
  Other events, or events with no time, are filtered."
  [dt & children]
  (spec/valid? ::scale [dt])
  {:action :throttle
   :params [dt]
   :children children})

(def action->fn
  {:above-dt cond-dt*
   :between-dt cond-dt*
   :decrement decrement*
   :coalesce coalesce*
   :critical critical*
   :critical-dt cond-dt*
   :debug debug*
   :ddt ddt*
   :ddt-pos ddt*
   :info info*
   :error error*
   :expired expired*
   :fixed-event-window fixed-event-window*
   :sflatten sflatten*
   :increment increment*
   :list-max list-max*
   :list-mean list-mean*
   :list-min list-min*
   :list-rate list-rate*
   :not-expired not-expired*
   :outside-dt cond-dt*
   :push-io! push-io!*
   :scale scale*
   :split split*
   :sdo sdo*
   :tag tag*
   :test-action test-action*
   :throttle throttle*
   :untag untag*
   :where where*
   :with with*})
