(ns mirabelle.db.queue
  (:require [clojure.core.protocols :as p]
            [corbihttp.log :as log]
            [com.stuartsierra.component :as component]
            [mirabelle.event :as event]
            [qbits.tape.appender :as appender]
            [qbits.tape.codec :as codec]
            [qbits.tape.queue :as queue]
            [qbits.tape.tailer :as tailer])
  (:import (net.openhft.chronicle.queue ExcerptAppender)
           (net.openhft.chronicle.bytes Bytes)))

(defn make
  "Creates a new appender instance.
  Takes a queue to append to as argument.
  You can also call datafy on the appender to get associated data."
  ([queue]
   (make queue nil))
  ([queue _]
   (let [codec (queue/codec queue)]
     (reify
       appender/IAppender
       (write! [_ x]
         (let [rw (Bytes/wrapForRead (codec/write codec x))
               ^ExcerptAppender appender (.acquireAppender (queue/underlying-queue queue))
               ret (with-open [ctx (.writingDocument appender)]
                     ;; Could throw if the queue is closed in another thread or on
                     ;; thread death: be paranoid here, we dont want to end up with
                     ;; a borked file, trigger rollback on any exception.
                     (try
                       (-> ctx .wire .write (.bytes rw))
                       (catch Throwable t
                         (.rollbackOnClose ctx)
                         t)))]
           (when (instance? Throwable ret)
             (throw (ex-info "Appender write failed"
                             {:type ::write-failed
                              :appender appender
                              :msg x}
                             ret)))
           ret))

       (last-index [_]
         (let [^ExcerptAppender appender (.acquireAppender (queue/underlying-queue queue))]
           (.lastIndexAppended appender)))

       (queue [_] queue)

       p/Datafiable
       (datafy [_]
         (let [^ExcerptAppender appender (.acquireAppender (queue/underlying-queue queue))]
           #::{:cycle (.cycle appender)
               :last-index-appended (try (.lastIndexAppended appender)
                                         ;; nothing was appended yet
                                         (catch java.lang.IllegalStateException _))
               :source-id (.sourceId appender)
               :queue queue}))))))

(defprotocol IQueue
  (write! [this event] "push an event in the queue")
  (read-all! [this action] "read all events from the queue, callling (action events) for each event."))

(defrecord ChroniqueQueue [directory ;; config
                           queue ;; runtime
                           appender ;;runtime
                           ]
  component/Lifecycle
  (start [this]
    (let [q (queue/make directory {:roll-cycle :hourly})
          a (make q)]
      (assoc this :queue q :appender a)))
  (stop [this]
    (when queue
      (queue/close! queue))
    (assoc this :queue nil :appender nil))
  IQueue
  (write! [this events]
    (appender/write! appender (event/sequential-events events)))
  (read-all! [this action]
    (let [start-time (System/currentTimeMillis)
          tailer (tailer/make queue)
          events-count (volatile! 0)
          continue? (volatile! true)
          run-fn (fn [event]
                   (vswap! events-count inc)
                   (action event))]
      (while @continue?
        (if-let [events (tailer/read! tailer)]
          (run! run-fn events)
          (vreset! continue? false)))
      (log/infof {}
                 "Read %s events in %s milliseconds"
                 @events-count
                 (- (System/currentTimeMillis) start-time)))))
