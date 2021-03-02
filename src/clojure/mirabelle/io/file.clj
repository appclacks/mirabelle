(ns mirabelle.io.file
  (:require [mirabelle.event :as event]
            [mirabelle.io :as io]))

(defrecord FileIO [path]
  io/IO
  (compare-io [this other]
    (= (:path this)
       (:path other)))
  (inject! [this event]
    (let [events (event/sequential-events event)]
      (doseq [event events]
        (spit path (str (pr-str event) "\n") :append true)))))
