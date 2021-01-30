(ns mirabelle.stream
  (:require [mirabelle.action :as action]))

(defn compile!
  [context stream]
  (when (seq stream)
    (for [s stream]
      (let [children (compile! context (:children s))
            func (get action/action->fn (:action s))
            params (:params s)]
        (if (seq params)
          (apply func context (concat params children))
          (apply func context children))))))

(defn compile-stream!
  "Compile a stream to functions and associate to it its entrypoint."
  [context stream]
  (assoc stream :entrypoint
         (->> [(:actions stream)]
              (compile! context)
              first)))

(defn stream!
  [stream event]
  ((:entrypoint stream) event))
