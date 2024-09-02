(ns user
  (:require [clojure.tools.namespace.repl :refer [refresh]]
            [mirabelle.core :as core]))

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
  (core/reload!)
  "reloaded")


