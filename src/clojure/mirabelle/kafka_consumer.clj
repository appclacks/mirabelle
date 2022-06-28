(ns mirabelle.kafka-consumer
  (:require [com.stuartsierra.component :as component]
            [mirabelle.stream :as stream])
  (:import org.apache.kafka.clients.consumer.KafkaConsumer
           java.util.ArrayList
           java.util.Collection
           java.util.Properties))

(defn build-properties
  [property-map]
  (let [prop (Properties.)]
    (doseq [[k v] property-map]
      (.setProperty prop (name k) (name v)))))


(defn build-consumer
  [config]
  (let [consumer (KafkaConsumer. ^Properties (build-properties (:properties config)))]
    (.subscribe consumer (ArrayList. ^Collection (:topics config)))))


(defn kafka-loop
  [consumer stream-handler running? auto-commit?]
  (while @running?
    ;; todo: deserializer
    ;; todo: also pass the offset to the event
    
    )
  )

{:topics ["foo" "bar"]
 :properties {"bootstrap.server" "..."}
 }



;; in Influx tags are indexed, not fields
(defrecord MirabelleKafkaConsumer [config
                                   stream-handler
                                   consumer
                                   running?
                                   future-result]
  component/Lifecycle
  (start [this]
    (let [running? (atom true)]
      (assoc this
             :future-result (future)
             :running? running?
             ))

    )
  (stop [this]
    (reset! running? false)
    @future-result
    ))
