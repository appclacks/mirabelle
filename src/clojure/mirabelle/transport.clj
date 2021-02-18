;; This code is from the Riemann code base
;; Copyright Riemann authors (riemann.io), thanks to them!
(ns mirabelle.transport
  (:require [clojure.tools.logging :as log]
            [mirabelle.time :as time]
            [mirabelle.stream :as stream]
            [riemann.codec  :as codec])
  (:import
    (java.util List)
    (java.util.concurrent TimeUnit)
    (io.riemann.riemann Proto$Msg)
    (io.netty.channel ChannelInitializer
                      Channel
                      ChannelPipeline)
    (io.netty.channel.group DefaultChannelGroup)
    (io.netty.channel.socket DatagramPacket)
    (io.netty.handler.codec MessageToMessageDecoder
                            MessageToMessageEncoder)
    (io.netty.handler.codec.protobuf ProtobufDecoder
                                     ProtobufEncoder)
    (io.netty.util ReferenceCounted)
    (io.netty.util.concurrent Future
                              EventExecutorGroup
                              DefaultEventExecutorGroup
                              ImmediateEventExecutor)))

(def ioutil-lock
  "There's a bug in JDK 6, 7, and 8 which can cause a deadlock initializing
  sse-server and netty concurrently; we serialize them with this lock.
  https://github.com/riemann/riemann/issues/617"
  (Object.))

(defn post-load-event
  "After events are loaded, we assign default times if none exist."
  [e]
  (if (:time e) e (assoc e :time (time/now))))

(defn decode-msg
  "Decode a protobuf to a message. Decodes the protocol buffer
  representation of Msg and applies post-load-event to all events."
  [msg]
  (let [msg (codec/decode-pb-msg msg)]
    (-> msg
      (assoc :states (map post-load-event (:states msg)))
      (assoc :events (map post-load-event (:events msg))))))

(defn ^DefaultChannelGroup channel-group
  "Make a channel group with a given name."
  [n]
  (DefaultChannelGroup. n (ImmediateEventExecutor/INSTANCE)))

(defn derefable
  "A simple wrapper for a netty future which on deref just calls
  (syncUninterruptibly f), and returns the future's result."
  [^Future f]
  (reify clojure.lang.IDeref
    (deref [_]
      (.syncUninterruptibly f)
      (.get f))))

(defn ^Future shutdown-event-executor-group
  "Gracefully shut down an event executor group. Returns a derefable future."
  [^EventExecutorGroup g]
  ; 10ms quiet period, 10s timeout.
  (derefable (.shutdownGracefully g 10 1000 TimeUnit/MILLISECONDS)))

(defn retain
  "Retain a ReferenceCounted object, if x is such an object. Otherwise, noop.
  Returns x."
  [x]
  (when (instance? ReferenceCounted x)
    (.retain ^ReferenceCounted x))
  x)

(defmacro channel-initializer
  "Constructs an instance of a Netty ChannelInitializer from a list of
  names and expressions which return handlers. Handlers with :shared metadata
  on their names are bound once and re-used in every invocation of
  getPipeline(), other handlers will be evaluated each time.

  ```clojure
  (channel-pipeline-factory
             frame-decoder    (make-an-int32-frame-decoder)
    ^:shared protobuf-decoder (ProtobufDecoder. (Proto$Msg/getDefaultInstance))
    ^:shared msg-decoder      msg-decoder)
  ```"
  [& names-and-exprs]
  (assert (even? (count names-and-exprs)))
  (let [handlers (partition 2 names-and-exprs)
        shared (filter (comp :shared meta first) handlers)
        pipeline-name (vary-meta (gensym "pipeline")
                                 assoc :tag `ChannelPipeline)
        forms (map (fn [[h-name h-expr]]
                     `(.addLast ~pipeline-name
                                ~(when-let [e (:executor (meta h-name))]
                                   e)
                                ~(str h-name)
                                ~(if (:shared (meta h-name))
                                   h-name
                                   h-expr)))
                   handlers)]
;    (prn forms)
    `(let [~@(apply concat shared)]
       (proxy [ChannelInitializer] []
         (initChannel [~'ch]
           (let [~pipeline-name (.pipeline ^Channel ~'ch)]
             ~@forms
             ~pipeline-name))))))

(defn protobuf-decoder
  "Decodes protobufs to Msg objects"
  []
  (ProtobufDecoder. (Proto$Msg/getDefaultInstance)))

(defn protobuf-encoder
  "Encodes protobufs to Msg objects"
  []
  (ProtobufEncoder.))

(defn datagram->byte-buf-decoder
  "A decoder that turns DatagramPackets into ByteBufs."
  []
  (proxy [MessageToMessageDecoder] []
    (decode [context ^DatagramPacket message ^List out]
      (.add out (retain (.content message))))

    (isSharable [] true)))

(defn msg-decoder
  "Netty decoder for Msg protobuf objects -> maps"
  []
  (proxy [MessageToMessageDecoder] []
    (decode [context message ^List out]
      (.add out (decode-msg message)))
    (isSharable [] true)))

(defn msg-encoder
  "Netty encoder for maps -> Msg protobuf objects"
  []
  (proxy [MessageToMessageEncoder] []
    (encode [context message ^List out]
      (.add out (codec/encode-pb-msg message)))
    (isSharable [] true)))

(defn event-executor
  "Creates a new netty execution handler for processing events. Defaults to 1
  thread per core."
  []
  (DefaultEventExecutorGroup. (.. Runtime getRuntime availableProcessors)))

(defn handle
  "Handles a msg with the given handler."
  [stream-handler msg]
  (try
    (doseq [event (:events msg)]
      (stream/push! stream-handler event
                    (or (:stream event)
                        :streaming)))
    {:ok true}

    ;; Some kind of error happened
    (catch Exception ^Exception e
      (log/error {} e)
      {:ok false :error (.getMessage e)})))
