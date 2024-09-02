(ns mirabelle.queue.queue
  (:require [taoensso.nippy :as nippy])
  (:import (net.openhft.chronicle.queue ChronicleQueue
                                        RollCycles
                                        ExcerptTailer
                                        ExcerptAppender)
           (java.nio ByteBuffer)
           (net.openhft.chronicle.bytes Bytes)
           (net.openhft.chronicle.queue.impl.single SingleChronicleQueueBuilder
                                                    SingleChronicleQueue))
  )

(defn make-queue
  [^String directory]
  (let [builder (ChronicleQueue/singleBuilder directory)]
    (.rollCycle builder RollCycles/FAST_DAILY)
    (.build builder)))

(defn queue->appender
  [^SingleChronicleQueue queue]
  (.acquireAppender queue))

(defn queue->tailer
  [^SingleChronicleQueue queue tailer-id]
  (.createTailer queue tailer-id))

(defn make
  [^String directory]
  (let [queue (make-queue directory)]
    {:appender (queue->appender queue)
     :tailer (queue->tailer queue "n1")}))

(defn write
  [^ExcerptAppender appender event]
  (.writeBytes appender (Bytes/wrapForRead ^"[B"(nippy/freeze event))
  ))

(defn read-queue
  [^ExcerptTailer tailer]
  (with-open [document (.readingDocument tailer)]
    (if (.isPresent document)
      (->> document .wire .read .bytes)
      "nod data"
      )

    )

  )

