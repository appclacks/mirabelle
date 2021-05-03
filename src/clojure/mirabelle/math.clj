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
      (if (zero? interval)
        (assoc base :metric sum)
        (assoc base :metric (/ sum interval))))))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn sorted-sample-extract
  "Returns the events in seqable s, sorted and taken at each point p of points,
  where p ranges from 0 (smallest metric) to 1 (largest metric). 0.5 is the
  median event, 0.95 is the 95th' percentile event, and so forth. Ignores
  events without a metric."
  [s points]
  (let [sorted (sort-by :metric (filter :metric s))]
    (if (empty? sorted)
      '()
      (let [n (count sorted)
            extract (fn [point]
                      (let [idx (min (dec n) (int (Math/floor (* n point))))]
                        (nth sorted idx)))]
        (map extract points)))))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn sorted-sample
  "Sample a sequence of events at points. Returns events with the :quantile
  key set to to \"pint\". For instance, (sorted-sample events [0 1])
  returns a 2-element seq of the smallest event and the biggest event, by
  metric. The first has a quantile set to \"0\" and the second one set to
  in \"1\". If the points is a map, eg (sorted-sample events {0 \".min\" 1
  \".max\"}, the the values will be appened to the quantile directly.
  Useful for extracting histograms and percentiles.

  When s is empty, returns an empty list."
  [s points]
  (map (fn [pname event]
         (assoc event :quantile pname))
       points
       (sorted-sample-extract s points)))
