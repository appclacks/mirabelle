;; Copyright Riemann authors (riemann.io), thanks to them!
(ns mirabelle.transport.codec
  "Encodes and decodes Riemann messages and events, between byte arrays,
  buffers, and in-memory types."
  (:require clojure.set)
  (:import [io.riemann.riemann Proto$Query Proto$Attribute Proto$Event Proto$Msg]
           [java.net InetAddress]
           [com.google.protobuf ByteString]))

(defrecord Msg [ok error events decode-time])

(defn decode-pb-metric
  [result ^Proto$Event e]
  (assoc result
         :metric
         (cond
           (.hasMetricSint64 e)    (.getMetricSint64 e)
           (.hasMetricD e)         (.getMetricD e)
           (.hasMetricF e)         (.getMetricF e))))

(defn decode-pb-attributes
  [result ^Proto$Event e]
  (assoc result
         :metric
         (cond
           (.hasMetricSint64 e)    (.getMetricSint64 e)
           (.hasMetricD e)         (.getMetricD e)
           (.hasMetricF e)         (.getMetricF e))))

(defn decode-pb-time
  [result ^Proto$Event e]
  (assoc result
         :time
         (cond
           (.hasTimeMicros e) (* (.getTimeMicros e) 1000) ;; time in us => nano
           (.hasTime e)       (* (.getTime e) 1000000000) ;; time in s => nano
           true               (* (System/currentTimeMillis)
                                 1000000))))

(defn decode-pb-event
  "Transforms a java protobuf to an Event."
  [^Proto$Event e]
  (cond-> {:attributes
           (cond-> {}
             (.hasHost e) (assoc :host (.getHost e))
             (pos? (.getAttributesCount e)) (into (map (fn [^Proto$Attribute a]
                                                         [(keyword (.getKey a)) (.getValue a)])
                                                       (.getAttributesList e))))}
    (.hasService e) (assoc :service (.getService e))
    (.hasState e) (assoc :state (.getState e))
    (.hasDescription e) (assoc :description (.getDescription e))
    (< 0 (.getTagsCount e)) (assoc :tags (vec (.getTagsList e)))
    (.hasTtl e) (assoc :ttl (.getTtl e))
    true (decode-pb-metric e)))

(defn decode-pb-msg
  "Transforms a java protobuf Msg to a defrecord Msg."
  [^Proto$Msg m]
  (let [t (System/nanoTime)]
    (Msg. (when (.hasOk m) (.getOk m))
          (when (.hasError m) (.getError m))
          (mapv decode-pb-event (.getEventsList m))
          t)))

(defn ^Proto$Msg encode-pb-msg
  "Transform a Protobuf Msg or a Clojure map to a java Protobuf Msg."
  [m]
  (if (instance? Proto$Msg m)
    m
    (let [msg (Proto$Msg/newBuilder)]
      (when-not (nil? (:ok m)) (.setOk msg (:ok m)))
      (when (:error m) (.setError msg (:error m)))
      (.build msg))))
