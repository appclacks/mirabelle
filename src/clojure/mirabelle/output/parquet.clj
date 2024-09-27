(ns mirabelle.output.parquet
  (:require [com.stuartsierra.component :as component]
            [mirabelle.io :as io])
  (:import io.opentelemetry.proto.trace.v1.Span$SpanKind
           org.apache.hadoop.fs.Path
           org.apache.hadoop.conf.Configuration
           org.apache.parquet.io.api.Binary
           org.apache.parquet.example.data.Group
           org.apache.parquet.example.data.simple.SimpleGroup
           org.apache.parquet.hadoop.ParquetWriter
           org.apache.parquet.hadoop.ParquetReader
           org.apache.parquet.hadoop.example.ExampleParquetWriter
           org.apache.parquet.hadoop.example.GroupReadSupport
           org.apache.parquet.hadoop.util.HadoopInputFile
           org.apache.parquet.schema.GroupType
           org.apache.parquet.schema.MessageType
           org.apache.parquet.schema.MessageTypeParser
           org.apache.parquet.schema.OriginalType
           org.apache.parquet.schema.Type
           org.apache.parquet.schema.Type$Repetition
           org.apache.parquet.schema.PrimitiveType
           org.apache.parquet.schema.PrimitiveType$PrimitiveTypeName
           org.apache.parquet.schema.LogicalTypeAnnotation
           org.apache.parquet.schema.LogicalTypeAnnotation$MapLogicalTypeAnnotation))

;;TODO
;; links?

;; message schema {
;;   optional binary traceID;
;;   optional binary spanID;
;;   optional binary parentSpanID;
;;   optional binary name;
;;   optional binary description;
;;   optional int64 startTime;
;;   required int64 time;
;;   optional int64 kind;
;;   optional int64 statusCode;
;;   optional binary serviceName;
;;   repeated group attributes {
;;     optional binary key;
;;     optional binary value;
;;   }
;;   repeated group resources {
;;     optional binary key;
;;     optional binary value;
;;   }
;;   repeated group events {
;;     optional binary name;
;;     optional int64 time;
;;     repeated group attributes {
;;       optional binary key;
;;       optional binary value;
;;     }
;;   }
;;   optional binary state;
;;   optional double metric;
;;   repeated group tags {
;;     optional binary tag;
;;   }
;; }

(def span-kind->kind
  {Span$SpanKind/SPAN_KIND_UNSPECIFIED "unspecified"
   Span$SpanKind/SPAN_KIND_INTERNAL "internal"
   Span$SpanKind/SPAN_KIND_SERVER "server"
   Span$SpanKind/SPAN_KIND_CLIENT "client"
   Span$SpanKind/SPAN_KIND_PRODUCER "producer"
   Span$SpanKind/SPAN_KIND_CONSUMER "consumer"})

(def kind->span-kind
  (clojure.set/map-invert span-kind->kind))

(def schema "message schema {\n  optional binary traceID;\n  optional binary spanID;\n  optional binary parentSpanID;\n  optional binary name;\n  optional binary description;\n  optional int64 startTime;\n  required int64 time;\n  optional int64 kind;\n  optional int64 statusCode;\n  optional binary serviceName;\n  repeated group attributes {\n    optional binary key;\n    optional binary value;\n  }\n  repeated group resources {\n    optional binary key;\n    optional binary value;\n  }\n  repeated group events {\n    optional binary name;\n    optional int64 time;\n    repeated group attributes {\n      optional binary key;\n      optional binary value;\n    }\n  }\n  optional binary state;\n  optional double metric;\n  repeated group tags {\n    optional binary tag;\n  }\n}\n")

(defn riemann-writer
  [path]
  (let [schema (MessageTypeParser/parseMessageType schema)
        path (Path. ^String path)
        builder (ExampleParquetWriter/builder path)
        writer (.build (.withType builder schema))]
    {:writer writer
     :schema schema}))

(comment
  (def output (component/start (map->ParquetOutput {:path "/tmp/foo.parquet"})))
  (io/inject! output
              [{:trace-id "abc-123"
                :span-id "13-ada"
                :parent-span-id "bae-123"
                :name "HTTP GET"
                :description "trololo"
                :start-time 10
                :time 35
                :span-kind 1
                :service "mirabelle"
                :status-code 1
                :attributes {:http.method "get"
                             :http.path "/foo"}
                :resources {:service.name "mirabelle"
                            :environment "prod"}
                :events [{:time 12
                          :name "event1"
                          :attributes {:my-attr "my-val"}}
                         {:time 14
                          :name "event2"
                          :attributes {:my-attr2 "my-va2"}}]
                :state "ok"
                :metric 10
                :tags ["foo" "bar"]
                }])
  (.close (:writer output))
  )

(defn event->riemann-group
  [{:keys [^MessageType schema]}]
  (let [attributes-group-type (.getType schema "attributes")
        resources-group-type (.getType schema "resources")
        tags-group-type (.getType schema "tags")
        events-group-type (.getType schema "events")
        events-attributes-group-type (.getType schema
                                              ^"[Ljava.lang.String;" (into-array String ["events" "attributes"]))]
    (fn [event]
      (let [group (SimpleGroup. schema)]
        (when-let [trace-id (:trace-id event)]
          (.add group "traceID" (Binary/fromString trace-id)))
        (when-let [span-id (:span-id event)]
          (.add group "spanID" (Binary/fromString span-id)))
        (when-let [parent-span-id (:parent-span-id event)]
          (.add group "parentSpanID" (Binary/fromString parent-span-id)))
        (when-let [n (:name event)]
          (.add group "name" (Binary/fromString n)))
        (when-let [start-time (:start-time event)]
         (.add group "startTime" (long start-time)))
        (when-let [event-time (:time event)]
          (.add group "time" (long event-time)))
        (when-let [kind (:span-kind event)]
          (.add group "kind" (long kind)))
        (when-let [status-code (:status-code event)]
          (.add group "statusCode" (long status-code)))
        (when-let [service (:service event)]
          (.add group "serviceName" (Binary/fromString service)))
        (when-let [description (:description event)]
          (.add group "description" (Binary/fromString description)))
        (when-let [attributes (:attributes event)]
          (doseq [[k v] attributes]
            (let [simple-group (SimpleGroup. attributes-group-type)]
              (.add simple-group "key" (Binary/fromString (name k)))
              (.add simple-group "value" (Binary/fromString v))
              (.add group "attributes" simple-group))))
        (when-let [resources (:resources event)]
          (doseq [[k v] resources]
            (let [simple-group (SimpleGroup. resources-group-type)]
              (.add simple-group "key" (Binary/fromString (name k)))
              (.add simple-group "value" (Binary/fromString v))
              (.add group "resources" simple-group))))
        (when-let [events (:events event)]
          (doseq [event events]
            (let [event-group (SimpleGroup. events-group-type)
                  event-attributes-group (SimpleGroup. events-attributes-group-type)]
              (.add event-group "time" (long (:time event)))
              (.add event-group "name" (Binary/fromString (:name event)))
              (doseq [[k v] (:attributes event)]
                (.add event-attributes-group "key" (Binary/fromString (name k)))
                (.add event-attributes-group "value" (Binary/fromString v)))
              (.add event-group "attributes" event-attributes-group)
              (.add group "events" event-group))))

        (when-let [state (:state event)]
          (.add group "state" (Binary/fromString state)))
        (when-let [metric (:metric event)]
          (.add group "metric" ^Double metric))
        (when-let [tags (:tags event)]
          (doseq [tag tags]
            (let [tags-group (SimpleGroup. tags-group-type)]
              (.add tags-group "tag" (Binary/fromString tag))
              (.add group "tags" tags-group))))

        group))))

(defn set-attributes
  [event ^SimpleGroup main-group]
  (let [size (.getFieldRepetitionCount main-group "attributes")]
    (if (pos? size)
      (loop [i 0
             result {}]
        (if (= i size)
          (assoc event :attributes result)
          (let [attribute-group (.getGroup main-group "attributes" i)]
            (recur (inc i)
                   (assoc result
                          (keyword (.getString attribute-group "key" 0))
                          (.getString attribute-group "value" 0))))))
      event)))

(defn set-resources
  [event ^SimpleGroup main-group]
  (let [size (.getFieldRepetitionCount main-group "resources")]
    (if (pos? size)
      (loop [i 0
             result {}]
        (if (= i size)
          (assoc event :resources result)
          (let [resource-group (.getGroup main-group "resources" i)]
            (recur (inc i)
                   (assoc result
                          (keyword (.getString resource-group "key" 0))
                          (.getString resource-group "value" 0))))))
      event)))

(defn set-events
  [event ^SimpleGroup main-group]
  (let [size (.getFieldRepetitionCount main-group "events")]
    (if (pos? size)
      (loop [i 0
             result []]
        (if (= i size)
          (assoc event :events result)
          (let [event-group (.getGroup main-group "events" i)]
            (recur (inc i)
                   (conj result
                         (set-attributes
                          {:name (.getString event-group "name" 0)
                           :time (.getLong event-group "time" 0)}
                          event-group))))))
      event)))

(defn span-kind
  [result span-kind]
  (assoc result
         :span-kind span-kind
         :kind (get span-kind->kind (Span$SpanKind/forNumber span-kind) "unrecognized")))

(defn set-tags
  [event ^SimpleGroup main-group]
  (let [size (.getFieldRepetitionCount main-group "tags")]
    (if (pos? size)
      (loop [i 0
             result []]
        (if (= i size)
          (assoc event :tags result)
          (let [tag-group (.getGroup main-group "tags" i)]
            (recur (inc i)
                   (conj result
                         (.getString tag-group "tag" 0))))))
      event)))

(defn read-parquet
  [^String path]
  (let [path (Path. path)
        file (HadoopInputFile/fromPath path (Configuration.))
        read-support (GroupReadSupport.)
        reader (.build (ParquetReader/builder read-support path))]
    (loop [^SimpleGroup main-group (.read reader)
           result []]
      (if main-group
        (recur (.read reader)
               (conj result
                     (cond-> (-> {:time (.getLong main-group "time" 0)}
                                 (set-attributes main-group)
                                 (set-resources main-group)
                                 (set-events main-group)
                                 (set-tags main-group))
                       (= 1 (.getFieldRepetitionCount main-group "traceID"))
                       (assoc :trace-id (.getString main-group "traceID" 0))
                       (= 1 (.getFieldRepetitionCount main-group "spanID"))
                       (assoc :span-id (.getString main-group "spanID" 0))
                       (= 1 (.getFieldRepetitionCount main-group "parentSpanID"))
                       (assoc :parent-span-id (.getString main-group "parentSpanID" 0))
                       (= 1 (.getFieldRepetitionCount main-group "name"))
                       (assoc :name (.getString main-group "name" 0))
                       (= 1 (.getFieldRepetitionCount main-group "startTime"))
                       (assoc :start-time (.getLong main-group "startTime" 0))
                       (= 1 (.getFieldRepetitionCount main-group "statusCode"))
                       (assoc :status-code (.getLong main-group "statusCode" 0))
                       (= 1 (.getFieldRepetitionCount main-group "kind"))
                       (span-kind (.getLong main-group "kind" 0))
                       (= 1 (.getFieldRepetitionCount main-group "serviceName"))
                       (assoc :service (.getString main-group "serviceName" 0))
                       (= 1 (.getFieldRepetitionCount main-group "description"))
                       (assoc :description (.getString main-group "description" 0))
                       (= 1 (.getFieldRepetitionCount main-group "state"))
                       (assoc :state (.getString main-group "state" 0))
                       (= 1 (.getFieldRepetitionCount main-group "metric"))
                       (assoc :metric (.getDouble main-group "metric" 0)))))
        (do (.close reader)
            result)))))

(defrecord ParquetOutput [path schema format group-fn ^ParquetWriter writer]
  component/Lifecycle
  (start [this]
    (let [{:keys [writer schema] :as riemann} (riemann-writer path)]
      (assoc this
             :writer writer
             :schema schema
             :group-fn (event->riemann-group riemann))))
  (stop [this]
    (.close writer)
    (dissoc this :writer :groups schema))
  io/Output
  (inject! [this events]
    (doseq [event events]
      (.write writer ^SimpleGroup (group-fn event)))))
