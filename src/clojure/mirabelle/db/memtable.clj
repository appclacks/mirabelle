(ns mirabelle.db.memtable
  (:require [com.stuartsierra.component :as component])
  (:import java.util.HashMap
           fr.mcorbin.mirabelle.memtable.Engine
           fr.mcorbin.mirabelle.memtable.Serie))

(def labels-hashmap
  (memoize (fn [labels-list values]
             (let [hashmap (HashMap.)]
               (doseq [label labels-list]
                 (.put hashmap (name label) (str (get values label))))
               hashmap))))

(defn event->serie
  "Takes an event and a list of labels. Return the Serie from them.

  (event->serie {:host \"foo\" :service \"bar} [:host])"
  [event labels-list]
  (Serie. (-> event :service name)
          (labels-hashmap labels-list (select-keys event labels-list))))

(defn ->serie
  "Build a serie from a service name and an hashmap

  (-> \"foo\" {:host \"mcorbin.fr\"})"
  [service hash-map]
  (Serie. service
          (let [hashmap (HashMap.)]
            (doseq [[k v] hash-map]
              (.put hashmap (name k) (str v)))
            hashmap)))

(defprotocol IMemtableEngine
  (inject! [this events labels] "Inject an event or a a list of events for a given serie in the memtable")
  (remove-serie [this service labels] "Remove a serie")
  (values [this service labels] "Returns all values for a given serie")
  (values-in [this service labels from to] "Returns all values for a given serie in the given interval"))

(defn inject-engine
  "Push an event into the memtable engine."
  [^Engine engine events labels-list]
  (let [events (if (sequential? events) events (list events))]
    (doseq [event events]
      (.add engine (:time event) (event->serie event labels-list) event))))

(defrecord MemtableEngine [memtable-max-ttl ;; config
                           memtable-cleanup-duration ;; config
                           ^Engine engine ;; runtime
                           ]
  component/Lifecycle
  (start [this]
    (assoc this :engine (Engine. memtable-max-ttl memtable-cleanup-duration)))
  (stop [this]
    (assoc this :engine nil))
  IMemtableEngine
  (inject! [this events labels-list]
    (inject-engine engine events labels-list))
  (remove-serie [this service labels]
    (.remove engine (->serie service labels)))
  (values [this service labels]
    (.valuesFor engine (->serie service labels)))
  (values-in [this service labels from to]
    (.valuesFor engine (->serie service labels) (double from) (double to))))
