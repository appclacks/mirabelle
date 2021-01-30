(ns mirabelle.io.file
  (:require [mirabelle.io :as io]))

(defrecord FileIO [path]
  io/IO
  (compare-io [this other]
    (= (:path this)
       (:path other)))
  (inject! [this event]
    (spit path (str (pr-str event) "\n") :append true)))
