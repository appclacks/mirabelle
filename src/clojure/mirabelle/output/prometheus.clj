(ns mirabelle.output.prometheus
  (:require [com.stuartsierra.component :as component]
            [corbihttp.log :as log]
            [mirabelle.io :as io]
            [mirabelle.output.http :as http]
            [mirabelle.output.batch :as batch]
            [mirabelle.prometheus :as prometheus]
            [mirabelle.time :as time])
  (:import org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient
           org.apache.hc.client5.http.async.methods.SimpleRequestBuilder
           org.apache.hc.client5.http.async.methods.SimpleHttpResponse
           org.apache.hc.core5.concurrent.FutureCallback
           org.apache.hc.core5.http.ContentType
           org.xerial.snappy.Snappy))

(def ^ContentType content-type (ContentType/create "application/x-protobuf"))

(defn events->request
  [^String url events]
  (let [^prometheus.Types$TimeSeries write-request (prometheus/events->write-request events)
        ^"[B" body (Snappy/compress (.toByteArray write-request))
        builder (doto (SimpleRequestBuilder/post url)
                 (.setBody body content-type)
                 (.setHeader "Content-Encoding" "snappy")
                 (.setHeader "User-Agent" "Mirabelle")
                 (.setHeader "X-Prometheus-Remote-Write-Version" "0.1.0"))]
    (.build builder)))

(defn ^FutureCallback response-callback
  []
  (reify FutureCallback
    (^void cancelled [_])
    (^void completed [_ response]
     ;(println "RESPONSE: " (.getCode response) " "(.getBodyText response))
     )
    (^void failed [_ ^Exception e]
     (log/error {}
       e
       "Prometheus client callback error"))))

(defrecord Prometheus [^String url
                       downstream-fn
                       ^CloseableHttpAsyncClient http-client
                       batcher
                       ]
  component/Lifecycle
  (start [this]
    (let [client ^CloseableHttpAsyncClient (http/async-client {})
          callback (response-callback)
          downstream-fn (fn [events]
                          (try
                            (.execute client
                                      ^SimpleHttpRequest
                                      (events->request url (sort-by :time events))
                                      callback)
                            (catch Exception e
                              (log/error {} e "Prometheus client error"))))]
      (assoc this
             :http-client client
             :batcher (component/start (batch/map->SimpleBatcher
                                        {:max-duration-ns (time/s->ns 5)
                                         :max-size 1000
                                         :downstream downstream-fn})))))
  (stop [this]
    (when batcher
      (component/stop batcher))
    (when http-client
      (.close http-client)))
  io/Output
  (inject! [_ events]
    (batch/inject! batcher events)))

