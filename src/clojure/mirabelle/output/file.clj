(ns mirabelle.output.file
  (:require [mirabelle.io :as io]))

(defrecord File [path]
  io/Output
  (inject! [_ events]
    (doseq [event events]
      (spit path (str (pr-str event) "\n") :append true))))
