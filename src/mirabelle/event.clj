(ns mirabelle.event
  (:require [mirabelle.time :as t]))

(defn most-recent
  "Get the most recent event from an event list"
  [events]
  (-> (sort-by :time events)
      last))

(defn expired?
  "Verifies if an event is expired"
  [current-time event]
  (or (= (:state event) "expired")
      (when-let [time (:time event)]
        (let [ttl (:ttl event t/default-ttl)
              age (- current-time time)]
          (> age ttl)))))

(defn critical?
  "Verifies if an event is critical"
  [event]
  (= (:state event) "critical"))
