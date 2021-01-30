(ns mirabelle.event
  (:require [mirabelle.time :as t]))

(defn most-recent
  "Get the most recent event from an event list"
  [events]
  (-> (sort-by :time events)
      last))

(defn expired?
  "Verifies if an event is expired"
  [event]
  (or (= (:state event) "expired")
      (when-let [time (:time event)]
        (let [ttl (:ttl event t/default-ttl)
              age (- (t/now) time)]
          (> age ttl)))))
