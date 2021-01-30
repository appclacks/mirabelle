(ns mirabelle.action
  (:require [mirabelle.event :as e]
            [mirabelle.log :as log]
            [mirabelle.math :as math]))

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
   :not= not=})

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
    (log/info {} (pr-str event))
    (call-rescue event children)))

(defn debug
  "Print the event in the logs

  ```clojure
  (increment
    (debug))
  ```"
  [& children]
  {:action :debug
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

(defn fixed-event-window
  "Returns a fixed-sized window of events

  ```clojure
  (fixed-event-window 5
    (debug))
  ```

  This example will return a vector events partitioned 5 by 5."
  [size & children]
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

(defn above
  [_ threshold dt & children]
  {:action :above
   :params [[:> :metric threshold] dt]
   :children children})

(def action->fn
  {:above cond-dt*
   :decrement decrement*
   :debug debug*
   :expired expired*
   :fixed-event-window fixed-event-window*
   :increment increment*
   :mean mean*
   :not-expired not-expired*
   :sdo sdo*
   :test-action test-action*
   :where where*})

(def stream
  [(where [:> :metric 10]
          (increment
           (debug))
          (increment
           (decrement
            (debug))
           )
          )]
  )
