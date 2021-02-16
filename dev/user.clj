(ns user
  (:require [clojure.tools.namespace.repl :refer [refresh]]
            [mirabelle.core :as core]
            [mirabelle.db.memtable :as memtable]))

(defn start!
  []
  (core/start!)
  "started")

(defn stop!
  []
  (core/stop!)
  "stopped")

(defn restart!
  []
  (stop!)
  (refresh)
  (start!))

(defn reload!
  []
  (core/reload!))

(defn mem-events
  [service labels]
  (let [memtable-engine (:memtable-engine @(:stream-handler core/system))]
    (memtable/values memtable-engine service labels
     )
    )
  )
