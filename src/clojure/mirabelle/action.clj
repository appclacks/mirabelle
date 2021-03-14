(ns mirabelle.action
  (:require [cheshire.core :as json]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [corbihttp.log :as log]
            [exoscale.ex :as ex]
            [mirabelle.action.condition :as cd]
            [mirabelle.db.queue :as queue]
            [mirabelle.event :as e]
            [mirabelle.index :as index]
            [mirabelle.io :as io]
            [mirabelle.math :as math]
            [mirabelle.spec :as spec])
  (:import java.util.concurrent.Executor))

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
    (fn [event]
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

(defn log-action
  "generic logger"
  [level]
  (fn [event]
    (when-let [event (keep-non-discarded-events event)]
      (condp = level
        :debug (log/debug {} (pr-str event))
        :info (log/info {} (pr-str event))
        :error (log/error {} (pr-str event))))))

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

(defn coll-mean*
  [_ & children]
  (fn [events]
    (call-rescue (math/mean events) children)))

(defn coll-mean
  "Computes the events mean (on metric).
  Should receive a list of events from the previous stream.
  The most recent event is used as a base to create the new event

  ```clojure
  (fixed-event-window 10
    (coll-mean
      (debug)))
  ```

  Computes the mean on windows of 10 events"
  [& children]
  {:action :coll-mean
   :children children})

(defn coll-max*
  [_ & children]
  (fn [events]
    (call-rescue (math/max-event events) children)))

(defn coll-max
  "Returns the event with the biggest metric.
  Should receive a list of events from the previous stream.

  ```clojure
  (fixed-event-window 10
    (coll-max
      (debug)))
  ```

  Get the event the biggest metric on windows of 10 events"
  [& children]
  {:action :coll-max
   :children children})

(defn coll-min*
  [_ & children]
  (fn [events]
    (call-rescue (math/min-event events) children)))

(defn coll-min
  "Returns the event with the smallest metric.
  Should receive a list of events from the previous stream.

  ```clojure
  (fixed-event-window 10
    (coll-min
      (debug)))
  ```

  Get the event the smallest metric on windows of 10 events"
  [& children]
  {:action :coll-min
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
  (let [condition-fn (cd/compile-conditions conditions)
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

(defn warning*
  [_ & children]
  (fn [event]
    (when (e/warning? event)
      (call-rescue event children))))

(defn warning
  "Keep all events in state warning"
  [& children]
  {:action :warning
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
  ;; discard io in test mode
  (if (:test-mode? context)
    (fn [_] nil)
    (if-let [io-component (get-in context [:io io-name :component])]
      (fn [event]
        (when-let [events (keep-non-discarded-events event)]
          (io/inject! io-component events)))
      (throw (ex/ex-incorrect (format "IO %s not found"
                                      io-name))))))

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
  (coalesce 10 [:host :service]
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

(defn coll-rate*
  [_  & children]
  (fn [events]
    (call-rescue (math/rate events) children)))

(defn coll-rate
  "Computes the rate on a list of events.
  Should receive a list of events from the previous stream.
  The latest event is used as a base to build the new event.

  ```clojure
  (fixed-event-window 3
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

  (tag \"foo\" (info))
  (tag [\"foo\" \"bar\"] (info))"
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
  "Passes on events where all tags are present. This stream returns true if an
  event it receives matches those tags, nil otherwise.

  Can be used as a predicate in a where form.

  ```clojure
  (tagged-all \"foo\" (info))
  (tagged-all [\"foo\" \"bar\"] (info))
  ```"
  [tags & children]
  (spec/valid? ::tagged-all [tags])
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
  (let [clauses (for [index (range (count clauses))]
                  [(nth clauses index) (nth children index)])
        comp-clauses (->> clauses
                          (map (fn [clause]
                                 [(cd/compile-conditions (first clause))
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
    (spec/valid? (s/coll-of ::condition) clauses-fn)
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

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn fixed-time-window*
  [_ n & children]
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
                         (< (:time event) (+ start-time n))
                         (-> (update state :buffer conj event)
                             (assoc :windows nil))

                         ; Above window
                         :else
                         (let [delta (- (:time event) start-time)
                               dstart (- delta (mod delta n))
                               empties (dec (/ dstart n))
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

(s/def ::fixed-time-window (s/cat :n pos-int?))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn fixed-time-window
  "A fixed window over the event stream in time. Emits vectors of events, such
  that each vector has events from a distinct n-second interval. Windows do
  *not* overlap; each event appears at most once in the output stream. Once an
  event is emitted, all events *older or equal* to that emitted event are
  silently dropped.

  Events without times accrue in the current window."
  [n & children]
  (spec/valid? ::scale [n])
  {:action :fixed-time-window
   :params [n]
   :children children})

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn moving-event-window*
  [_ n & children]
  (let [window (atom (vec []))]
    (fn stream [event]
      (let [w (swap! window (fn swap [w]
                              (vec (take-last n (conj w event)))))]
        (call-rescue w children)))))

(s/def ::moving-event-window (s/cat :n pos-int?))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn moving-event-window
  "A sliding window of the last few events. Every time an event arrives, calls
  children with a vector of the last n events, from oldest to newest. Ignores
  event times. Example:

  ```clojure
  (moving-event-window 5 (coll-mean (info))
  ```"
  [n & children]
  (spec/valid? ::moving-event-window [n])
  {:action :moving-event-window
   :params [n]
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
  (spec/valid? ::ewma-timeless [r])
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
  "Passes on events only when their metric is greater than x"
  [n & children]
  (spec/valid? ::over [n])
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
  "Passes on events only when their metric is greater than x"
  [n & children]
  (spec/valid? ::under [n])
  {:action :under
   :params [n]
   :children children})

(defn changed*
  [_ field init & children]
  (let [state (atom [init nil])]
    (fn [event]
      (let [[_ event] (swap! state
                             (fn [s]
                               (let [current-val (get event field)]
                                 (if (= (first s)
                                        current-val)
                                   [(first s) nil]
                                   [current-val event]))))]
        (when event
          (call-rescue event children))))))

(s/def ::changed (s/cat :field keyword? :init any?))

(defn changed
  "Passes on events only if the `field` passed as parameter differs
  from the previous one.
  The `init` parameter is the default value for the stream.

  ```clojure
  (changed :state \"ok\")
  ```

  For example, this action will let event pass if the :state field vary,
  the initial value being `ok`.

  This stream is useful to get only events making a transition."
  [field init & children]
  (spec/valid? ::changed [field init])
  {:action :changed
   :params [field init]
   :children children})

(defn project*
  [_ conditions & children]
  (let [conditions-fns (map cd/compile-conditions conditions)
        state (atom {:buffer (reduce #(assoc %1 %2 nil)
                                     {}
                                     (range 0 (count conditions)))
                     :current-time 0})]
    (fn [event]
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
        (when (seq events)
          (call-rescue events children))))))

(defn index*
  [context labels]
  (let [i (:index context)]
    (fn [event]
      (index/insert i event labels))))

(s/def ::index (s/cat :labels (s/coll-of keyword?)))

(defn index
  "Insert events into the index."
  [labels]
  (spec/valid? ::index [labels])
  {:action :index
   :params [labels]})

(defn coll-count*
  [_ & children]
  (fn [events]
    ;; send empty event if the list is empty
    (call-rescue (or (math/count-events events)
                     {:metric 0})
                 children)))

(defn coll-count
  "Count the number of events.
  Should receive a list of events from the previous stream.
  The most recent event is used as a base to create the new event, and
  its :metric field is set to the number of events received as input."
  [& children]
  {:action :coll-count
   :children children})

(s/def ::sdissoc (s/cat :sdissoc (s/or :single keyword?
                                       :multiple (s/coll-of keyword?))))

(defn sdissoc*
  [_ fields & children]
  (fn [event]
    (call-rescue (apply dissoc event fields)
                 children)))

(defn sdissoc
  "Remove a key (or a list of keys) from the event

  ```clojure
  (sdissoc :host (info))

  (sdissoc [:environment :host] (info))
  "
  [fields & children]
  (spec/valid? ::sdissoc [fields])
  {:action :sdissoc
   :params [(if (keyword? fields) [fields] fields)]
   :children children})

(defn percentiles*
  [_ points & children]
  (fn [events]
    (doseq [event (math/sorted-sample events points)]
      (call-rescue event
                   children))))

(s/def ::percentiles (s/cat :points (s/coll-of number?)))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn percentiles
  "Over each period of interval seconds, aggregates events and selects one
  event from that period for each point. If point is 0, takes the lowest metric
  event.  If point is 1, takes the highest metric event. 0.5 is the median
  event, and so forth. Forwards each of these events to children. The event
  has the point appended the `:quantile` key.
  Useful for extracting histograms and percentiles.

  ```clojure
  (fixed-event-window 10
    (percentiles [0.5 0.75 0.98 0.99]))
  ```"
  [points & children]
  (spec/valid? ::percentiles [points])
  {:action :percentiles
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
    (moving-time-window 5))
  ```

  Generates a moving window for each host/service combination."
  [fields & children]
  (spec/valid? ::by [fields])
  {:action :by
   :params [fields]
   :children children})

(defn restore!*
  [context]
  (let [queue (:queue context)]
    (fn [events]
      (when-let [events (keep-non-discarded-events events)]
        (queue/write! queue events)))))

(defn restore!
  "Write events into the on-disk queue."
  []
  {:action :restore!
   :params []})

(defn reinject!*
  [context destination-stream]
  (let [reinject-fn (:reinject context)]
    (fn [event]
      (reinject-fn event destination-stream))))

(s/def ::reinject (s/cat :destination-stream keyword?))

(defn reinject!
  "Reinject an event into the streaming system."
  ([]
   (reinject! :streaming))
  ([destination-stream]
   (spec/valid? ::reinject [destination-stream])
   {:action :reinject!
    :params [destination-stream]}))

(s/def ::async-queue! (s/cat :queue-name keyword?))

(defn async-queue!*
  [context queue-name & children]
  (if (:test-mode? context)
    (apply sdo* context children)
    (if-let [^Executor executor (get-in context [:io queue-name :component])]
      (fn [event]
        (.execute executor
                  (fn []
                    (call-rescue event children))))
      (throw (ex/ex-incorrect (format "Async queue %s not found"
                                      queue-name))))))

(defn async-queue!
  "Execute children into the specific async queue."
  [queue-name & children]
  (spec/valid? ::async-queue! [queue-name])
  {:action :async-queue!
   :params [queue-name]
   :children children})

(defn io*
  [context & children]
  (if (:test-mode? context)
    (fn [_] nil)
    (apply sdo* context children)))

(defn io
  "Discard all events in test mode. Else, forward to children"
  [& children]
  {:action :io
   :children children})

(defn tap*
  [context tape-name]
  (if (:test-mode? context)
    (let [tap (:tap context)]
      (fn [event]
        (swap! tap
               (fn [tap]
                 (update tap tape-name (fn [v] (if v (conj v event) [event])))))))
    ;; discard in non-tests
    (fn [_] nil)))

(s/def ::tap (s/cat :tap-name keyword?))

(defn tap
  "Save events into the tap. Noop outside tests"
  [tap-name]
  (spec/valid? ::tap [tap-name])
  {:action :tap
   :params [tap-name]})

(defn json-fields*
  [_ fields & children]
  (fn [event]
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
  fields from json to edn."
  [fields & children]
  (spec/valid? ::json-fields [fields])
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
  (fn [event]
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
    (alert))
  ```"
  [& children]
  (when-not (= 2 (count children))
    (ex/ex-incorrect! "The exception-stream action should take 2 children"
                      {}))
  {:action :exception-stream
   :children children})

(defn stream
  [config & children]
  (-> (assoc config :actions (apply sdo children))))

(defn streams
  [& streams]
  (reduce
   (fn [state stream-config]
     (assoc state (:name stream-config) (dissoc stream-config :name)))
   {}
   streams))

(def action->fn
  {:above-dt cond-dt*
   :async-queue! async-queue!*
   :between-dt cond-dt*
   :changed changed*
   :coalesce coalesce*
   :coll-max coll-max*
   :coll-mean coll-mean*
   :coll-min coll-min*
   :coll-rate coll-rate*
   :coll-count coll-count*
   :critical critical*
   :critical-dt cond-dt*
   :debug debug*
   :decrement decrement*
   :ddt ddt*
   :ddt-pos ddt*
   :info info*
   :error error*
   :ewma-timeless ewma-timeless*
   :exception-stream exception-stream*
   :expired expired*
   :fixed-event-window fixed-event-window*
   :fixed-time-window fixed-time-window*
   :increment increment*
   :index index*
   :io io*
   :json-fields json-fields*
   :moving-event-window moving-event-window*
   :not-expired not-expired*
   :outside-dt cond-dt*
   :over over*
   :percentiles percentiles*
   :push-io! push-io!*
   :reinject! reinject!*
   :scale scale*
   :sflatten sflatten*
   :split split*
   :sdissoc sdissoc*
   :sdo sdo*
   :tag tag*
   :tagged-all tagged-all*
   :tap tap*
   :test-action test-action*
   :throttle throttle*
   :under under*
   :untag untag*
   :warning warning*
   :where where*
   :with with*
   :restore! restore!*})
