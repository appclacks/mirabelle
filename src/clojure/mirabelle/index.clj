;; This code is from the Riemann code base (and slighty adapted to work with Mirabelle)
;; Copyright Riemann authors (riemann.io), thanks to them!
(ns mirabelle.index
  (:require [com.stuartsierra.component :as component]
            [corbihttp.log :as log]
            [mirabelle.action.condition :as cd]
            [mirabelle.time :as t])
  (:import (org.cliffc.high_scale_lib NonBlockingHashMap)))

(defprotocol IIndex
  (size-index [this]
    "Returns the index size")
  (clear-index [this]
    "Resets the index")
  (current-time [this]
    "Returns the index current time")
  (new-time? [this t]
    "Takes a number representing the time as parameter and set the index current time if necessary")
  (delete [this labels]
    "Deletes an event by labels.")
  (expire [this]
    "Return a seq of expired states from this index, removing each.")
  (search [this condition]
    "Returns a seq of events from the index matching this query.")
  (insert [this event labels]
    "Updates index with event")
  (lookup [this labels]
    "Lookup an indexed event from the index"))

(defrecord Index [^NonBlockingHashMap index current-time]
  component/Lifecycle
  (start [this]
    (assoc this
           :index (NonBlockingHashMap.)
           :current-time (atom 0)))
  (stop [this]
    (assoc this
           :index nil
           :current-time nil))
  IIndex
  (size-index [this]
    (.size index))
  (clear-index [this]
    (.clear index))
  (current-time [this]
    @current-time)
  (new-time? [this t]
    (swap! current-time (fn [current] (max t current))))
  (delete [this labels]
    (.remove index labels))
  (expire [this]
    (reduce
     (fn [state ^java.util.Map$Entry map-entry]
       (let [labels (.getKey map-entry)
             event (.getValue map-entry)]
         (try
           (let [age (- @current-time (:time event))
                 ttl (or (:ttl event) t/default-ttl)]
             (println "age " age)
             (println "ttl " ttl)
             (when (< ttl age)
               (delete this labels)
               (conj state event)))
           (catch Exception e
             (log/error {}
                        e
                        (format "Caught exception while trying to expire labels %s, event %s"
                                (pr-str labels)
                                (pr-str event)))
             (delete this labels)
             state))))
     []
     index))

  (search [this condition]
    (let [condition-fn (cd/compile-conditions condition)]
      (filter condition-fn (.values index))))

  (insert [this event labels]
    (when (:time event)
      (if (= "expired" (:state event))
        (delete this labels)
        (.put index (select-keys event labels) event))))

  (lookup [this labels]
    (.get index labels)))
