(ns mirabelle.io.file
  (:require [mirabelle.io :as io]))

(defrecord FileIO [path]
  io/IO
  (inject! [this events]
    (doseq [event events]
      (spit path (str (pr-str event) "\n") :append true))))
