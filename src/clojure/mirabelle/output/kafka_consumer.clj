(ns mirabelle.io.kafka-consumer
  (:import java.util.Properties))

(defn build-properties
  [property-map]
  )

{:topics ["foo" "bar"]
 :properties {"bootstrap.server" "..."}
 }

;; in Influx tags are indexed, not fields
(defrecord KafkaConsumer [config
                          stream-handler
                          consumer
                          running
                          future-result]
  component/Lifecycle
  (start [this]
    (let [running (atom true)]
      (assoc this
             :future-result (future)
             :running running
             ))

    )
  (stop [this]
    (reset! running false)
    @future-result
    ))
