(ns mirabelle.output.influxdb
  (:require [clojure.spec.alpha :as s]
            [clojure.tools.logging :as log]
            [corbihttp.spec :as spec]
            [com.stuartsierra.component :as component]
            [exoscale.cloak :as cloak]
            [mirabelle.io :as io]
            [mirabelle.spec :as mspec])
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
                     (.toCharArray ^String (cloak/unmask (:password config)))))
    (when-let [token (:token config)]
      (log/info "Using token authentication for influxdb")
      (.authenticateToken options (.toCharArray ^String (cloak/unmask token))))
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

(def keys-to-remove-tags #{:metric :time :tags})

(defn event->point
  "Converts an event to an InfluxDB Point."
  [config event]
  (let [fields (select-keys event (:influxdb/fields event (:fields config)))
        measurement (:service event)
        point (Point/measurement measurement)]
    (if-let [tags (:influxdb/tags event)]
      (doseq [k tags]
        (.addTag point (name k) (get event k)))
      (doseq [[k v] event]
        (when (and (not (contains? fields k))
                   (not (keys-to-remove-tags k)))
          (.addTag point (name k) v))))
    (doseq [[k v] fields]
      (.addField point
                 ^String (name k)
                 (converts-double v)))
    (.time point
           ^Long (long (* 1000000 (:time event)))
           WritePrecision/US)
    point))

(s/def ::connection-string ::spec/ne-string)
(s/def ::bucket ::spec/ne-string)
(s/def ::org ::spec/ne-string)
(s/def ::username ::spec/ne-string)
(s/def ::password ::spec/secret)
(s/def ::token ::spec/secret)
(s/def ::fields (s/coll-of keyword?))
(s/def ::default-tags (s/map-of ::spec/keyword-or-str
                                ::spec/keyword-or-str))

(def default-config
  {:fields []})

(s/def ::influxdb (s/keys :req-un [::connection-string
                                   ::bucket
                                   ::org]
                          :opt-un [::username
                                   ::password
                                   ::fields
                                   ::token
                                   ::default-tags]))

;; in Influx tags are indexed, not fields
(defrecord Influx [config
                   ^InfluxDBClient client
                   ^WriteApi write-api
                   ]
  component/Lifecycle
  (start [this]
    (mspec/valid? ::influxdb config)
    (if client
      this
      (let [config (merge default-config config)
            ^InfluxDBClient c (influxdb-client config)]
        (assoc this
               :config config
               :client c
               :write-api (.getWriteApi c)))))
  (stop [this]
    (.close client)
    (assoc this :client nil :write-api nil))
  io/Output
  (inject! [_ events]
    (doseq [event events]
      (.writePoint write-api (event->point config event)))))


