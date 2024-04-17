(ns mirabelle.output.prometheus
  (:require [clj-http.client :as client]
            [mirabelle.prometheus :as prometheus])
  (:import prometheus.Remote$WriteRequest
           prometheus.Types$TimeSeries
           prometheus.Types$Label
           prometheus.Types$Sample))

(defrecord Prometheus [config
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
