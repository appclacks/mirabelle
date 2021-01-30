(ns mirabelle.action
  (:require [clojure.spec.alpha :as s]
            [corbihttp.log :as log]
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
  (let [condition-fn (get condition->fn condition)]
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

(defn mean*
  [_ & children]
  (fn [events]
    (call-rescue (math/mean events) children)))

(defn mean
  [& children]
  {:action :mean
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
  (fn [event]
    (when (e/expired? event)
      (call-rescue event children))))

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
  (fn stream [event]
    (when (not (e/expired? event))
      (call-rescue event children))))

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
  (s/def ::push-io! (s/cat :io-name keyword?))
  {:action :push-io!
   :params [io-name]})

(def action->fn
  {:above-dt cond-dt*
   :between-dt cond-dt*
   :decrement decrement*
   :critical critical*
   :critical-dt cond-dt*
   :debug debug*
   :info info*
   :error error*
   :expired expired*
   :fixed-event-window fixed-event-window*
   :increment increment*
   :mean mean*
   :not-expired not-expired*
   :outside-dt cond-dt*
   :push-io! push-io!*
   :sdo sdo*
   :test-action test-action*
   :where where*})
