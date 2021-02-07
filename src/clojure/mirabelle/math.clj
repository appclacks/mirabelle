(ns mirabelle.math
  (:require [mirabelle.event :as event]))

(defn mean
  "Takes a list of event and returns the metrics mean.
  The latest event is used as a base to build the event returned by
  this funcion."
  [events]
  (when (seq events)
    (assoc (event/most-recent events)
           :metric
           (-> (reduce #(+ (:metric %2) %1) 0 events)
               (/ (count events))))))

(defn count-events
  "Count the number of events.
  The latest event is used as a base to build the event returned by
  this funcion."
  [events]
  (when (seq events)
    (assoc (event/most-recent events)
           :metric
           (count events))))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn extremum
  [operation events]
  (when (seq events)
    (reduce
     (fn [state event]
       (cond
         (not (:metric event))
         state

         (and (nil? state) (:metric event))
         event

         (operation (:metric event) (:metric state))
         event

         :else
         state))
     nil
     events)))

(defn max-event
  "Takes a list of event and returns the event with
  the biggest metric"
  [events]
  (extremum >= events))

(defn min-event
  "Takes a list of event and returns the event with
  the smallest metric"
  [events]
  (extremum <= events))

(defn rate
  "Takes a list of events and compute the rate for them.
  Use the most recent event as a base for the new event."
  [events]
  (when (seq events)
    (let [{:keys [base sum min-time]}
          (reduce
           (fn [{:keys [base sum min-time]} event]
             (let [min-time (min (:time event) min-time)
                   new-state {:min-time min-time
                              :sum (+ (:metric event) sum)}]
               (cond
                 (not base)
                 (assoc new-state :base event)

                 (>= (:time event) (:time base))
                 (assoc new-state :base event)

                 :else
                 (assoc new-state :base base))))
           {:sum 0
            :min-time (:time (first events))}
           events)
          interval (- (:time base) min-time)]
      (if (= 0 interval)
        (assoc base :metric sum)
        (assoc base :metric (/ sum (- (:time base) min-time)))))))
