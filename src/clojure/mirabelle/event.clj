(ns mirabelle.event
  (:require [mirabelle.time :as t]
            [clojure.set :as set]))

(defn most-recent
  "Get the most recent event from an event list"
  [events]
  (-> (sort-by :time events)
      last))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn expired?
  "Verifies if an event is expired"
  [current-time event]
  (or (= (:state event) "expired")
      (when-let [time (:time event)]
        (let [ttl (:ttl event t/default-ttl)
              age (- current-time time)]
          (> age ttl)))))

(defn most-recent?
  "Returns true if event 1 is most recent than event 2.
  Does not work with events with no time."
  [event1 event2]
  (> (:time event1)
     (:time event2)))

(defn most-recent-event
  "Returns the most recent event between both"
  [event1 event2]
  (if (most-recent? event1 event2)
    event1
    event2))

(defn critical?
  "Verifies if an event is critical"
  [event]
  (= (:state event) "critical"))

(defn warning?
  "Verifies if an event is warning"
  [event]
  (= (:state event) "warning"))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn tagged-all?
  "Predicate function to check if a collection of tags is
  present in the tags of event."
  [tags event]
  (set/subset? (set tags) (set (:tags event))))

(defn sequential-events
  [event]
  (if (sequential? event) event (list event)))
