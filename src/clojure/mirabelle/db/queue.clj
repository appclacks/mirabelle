(ns mirabelle.db.queue
  (:require [com.stuartsierra.component :as component]
            [qbits.tape.appender :as appender]
            [qbits.tape.queue :as queue]))


(defprotocol IQueue
  (write! [this event] "push an event in the queue"))

(defrecord ChroniqueQueue [directory ;; config
                           queue ;; runtime
                           appender ;;runtime
                           ]
  component/Lifecycle
  (start [this]
    (let [q (queue/make directory {:roll-cycle :hourly})
          a (appender/make q)]
      (assoc this :queue q :appender a)))
  (stop [this]
    (when queue
      (queue/close! queue))
    (assoc this :queue nil :appender nil))
  IQueue
  (write! [this events]
    (appender/write! appender events)
    )
  )
