(ns mirabelle.io.influxdb
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [com.stuartsierra.component :as component]
            [mirabelle.io :as io]
            [mirabelle.spec :as spec])
  (:import com.influxdb.client.InfluxDBClient
           com.influxdb.client.InfluxDBClientFactory
           com.influxdb.client.InfluxDBClientOptions
           com.influxdb.client.InfluxDBClientOptions$Builder
           com.influxdb.client.WriteApi
           com.influxdb.client.domain.WritePrecision
           com.influxdb.client.write.Point))

(defn influxdb-options
  [config]
  (let [options (doto (InfluxDBClientOptions$Builder.)
                  (.connectionString (:connection-string config))
                  (.bucket (:bucket config))
                  (.org (:org config)))]
    (when (and (:username config) (:password config))
      (log/info "Using username/password authentication for influxdb")
      (.authenticate options
                     (:username config)
                     (.toCharArray ^String (:password config))))
    (when-let [token (:token config)]
      (log/info "Using token authentication for influxdb")
      (.authenticateToken options (.toCharArray ^String token)))
    (when-let [tags (:default-tags config)]
      (doseq [[tag value] tags]
        (.addDefaultTag options (name tag) (name value))))
    ;; todo log level
    (.build options)))

(defn influxdb-client
  "Build an InfluxDB client from a configuration map."
  [config]
  (let [^InfluxDBClientOptions options (influxdb-options config)]
    (InfluxDBClientFactory/create options)))

(defn converts-double
  "if n if a ratio, converts it to double. Returns n otherwise."
  [n]
  (if (ratio? n)
    (double n)
    n))

(defn event->point
  "Converts an event to an InfluxDB Point."
  [config event]
  (let [tags (select-keys event
                          (apply conj
                                 (:tags config)
                                 (:influxdb/tags event)))
        fields (select-keys event
                            (apply conj
                                   (:fields config)
                                   (:influxdb/fields event)))
        measurement (get event (or (:influxdb/measurement event)
                                   (:measurement config)))
        point (Point/measurement measurement)]
    (doseq [[k v] tags]
      (.addTag point (name k) v))
    (doseq [[k v] fields]
      (.addField point (name k) (converts-double v)))
    (.time point
           ^Long (long (* 1000000 (:time event)))
           WritePrecision/US)
    point))

(s/def ::connection-string ::spec/ne-string)
(s/def ::bucket ::spec/ne-string)
(s/def ::org ::spec/ne-string)
(s/def ::username ::spec/ne-string)
(s/def ::password ::spec/ne-string)
(s/def ::token ::spec/ne-string)
(s/def ::tags (s/coll-of keyword?))
(s/def ::fields (s/coll-of keyword?))
(s/def ::measurement keyword?)
(s/def ::default-tags (s/map-of ::spec/keyword-or-str
                                ::spec/keyword-or-str))

(def default-config
  {:tags []
   :fields []})

(s/def ::influxdb (s/keys :req-un [::connection-string
                                   ::bucket
                                   ::org]
                          :opt-un [::username
                                   ::tags
                                   ::measurement
                                   ::fields
                                   ::password
                                   ::token
                                   ::default-tags]))

;; in Influx tags are indexed, not fields
(defrecord InfluxIO [config
                     ^InfluxDBClient client
                     ^WriteApi write-api
                     ]
  component/Lifecycle
  (start [this]
    (spec/valid? ::influxdb config)
    (let [config (merge default-config config)
          ^InfluxDBClient c (influxdb-client config)]
      (assoc this
             :config config
             :client c
             :write-api (.getWriteApi c))))
  (stop [this]
    (.close client)
    (assoc this :client nil :write-api nil))
  io/IO
  (inject! [this events]
    (doseq [event events]
      (.writePoint write-api (event->point config event)))))


