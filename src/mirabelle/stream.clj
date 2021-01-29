(ns mirabelle.stream
  (:require [mirabelle.action :as action]))

(defn compile!
  "Build a list of executable functions from a stream definition"
  [context stream]
  (when (seq stream)
    (for [s stream]
      (let [children (compile! context (:children s))
            func (get action/action->fn (:action s))
            params (:params s)]
        (if (seq params)
          (apply func context (concat params children))
          (apply func context children))))))
