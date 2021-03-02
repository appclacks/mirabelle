(ns mirabelle.db.queue
  (:require [clojure.core.protocols :as p]
            [com.stuartsierra.component :as component]
            [qbits.tape.appender :as appender]
            [qbits.tape.codec :as codec]
            [qbits.tape.queue :as queue])
  (:import (net.openhft.chronicle.queue ChronicleQueue
                                        ExcerptAppender)
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
                       (.index ctx)
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
  (write! [this event] "push an event in the queue"))

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
    (appender/write! appender events)))
