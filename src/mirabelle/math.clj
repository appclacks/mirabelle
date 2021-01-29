(ns mirabelle.math
  (:require [mirabelle.event :as event]))

(defn mean
  "Takes a list of event and returns the metrics mean.
  The latest event is used as a base to build the event returned by
  this function."
  [events]
  (when (seq events)
    (assoc (event/most-recent events)
           :metric
           (-> (reduce #(+ (:metric %2) %1) 0 events)
               (/ (count events))))))
