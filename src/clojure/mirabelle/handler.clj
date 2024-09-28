(ns mirabelle.handler
  (:require [byte-streams :as bs]
            [clojure.edn :as edn]
            [com.stuartsierra.component :as component]
            [corbihttp.metric :as metric]
            [exoscale.ex :as ex]
            [mirabelle.b64 :as b64]
            [mirabelle.otel.traces :as traces]
            [mirabelle.prometheus :as prometheus]
            [mirabelle.stream :as stream]
            [mirabelle.time :as time])
  (:import io.micrometer.core.instrument.Counter
           io.micrometer.prometheusmetrics.PrometheusMeterRegistry
           io.opentelemetry.proto.collector.trace.v1.ExportTraceServiceRequest
           org.xerial.snappy.Snappy
           prometheus.Remote$WriteRequest))

(defprotocol IHandler
  (healthz [this params] "Healthz handler")
  (add-stream [this params] "Add a new stream")
  (get-stream [this params] "Get a stream")
  (remove-stream [this params] "Remove a stream")
  (list-streams [this params] "List streams")
  (push-event [this params] "Push an event to a stream")
  (prom-remote-write [this params] "Prometheus remote write endpoint")
  (fluentbit [this params] "fluentbit http log endpoint")
  (otel-traces [this params] "Opentelemetry traces v1 endpoint")
  (not-found [this params] "Not found handler")
  (metrics [this params] "Return the metrics"))

(defrecord Handler [stream-handler
                    ^PrometheusMeterRegistry registry
                    ^Counter prom-counter]
  component/Lifecycle
  (start [this]
    (assoc this :prom-counter (metric/get-counter! registry
                                                   :prometheus-remote-write
                                                   {})))
  (stop [this]
    (assoc this :prom-counter nil))
  IHandler
  (healthz [_ _]
    {:status 200
     :body {:message "ok"}})
  (add-stream [_ {:keys [all-params]}]
    (let [stream-name (:name all-params)
          config (-> all-params :config b64/from-base64 edn/read-string)]
      (stream/add-stream stream-handler stream-name config)
      {:status 200
       :body {:message "stream added"}}))
  (push-event [_ {:keys [all-params]}]
    (let [stream-name (:name all-params)]
      (stream/push! stream-handler
                    (time/default-time (:event all-params))
                    stream-name)
      {:status 200
       :body {:message "ok"}}))
  (remove-stream [_ {:keys [all-params]}]
    (let [stream-name (:name all-params)]
      (stream/remove-stream stream-handler stream-name)
      {:status 200
       :body {:message "stream removed"}}))
  (get-stream [_ {:keys [all-params]}]
    (let [stream-name (:name all-params)
          stream (stream/get-stream stream-handler stream-name)
          config (-> stream
                     (dissoc :context :entrypoint)
                     pr-str
                     b64/to-base64)]
      {:status 200
       :body {:config config}}))
  (list-streams [_ _]
    {:status 200
     :body {:streams (stream/list-streams stream-handler)}})
  (prom-remote-write [_ {:keys [all-params] :as request}]
    (let [^java.io.InputStream body (:body request)
          ^"[B" body-bytes (bs/to-byte-array body)
          ^"[B" uncompressed-body (Snappy/uncompress body-bytes)
          ^Remote$WriteRequest write-request (Remote$WriteRequest/parseFrom
                                              uncompressed-body)
          events-series (prometheus/write-request->events write-request)
          stream-name (:name all-params)]
      (.increment ^Counter prom-counter (.getTimeseriesCount write-request))
      (doseq [serie events-series]
        (doseq [event serie]
          (stream/push! stream-handler event stream-name)))
      {:status 200}))
  (fluentbit [_ {:keys [body path-params] :as request}]
    (let [stream-name (:name path-params)]
      (doseq [log (:body request)]
        (stream/push! stream-handler
                      (-> log (assoc :time (:date log)) (dissoc :date))
                      (keyword stream-name))))
    {:status 200})
  (otel-traces [_ {:keys [all-params] :as request}]
    (let [stream-name (:name all-params)
          ^java.io.InputStream body (:body request)
          ^"[B" body-bytes (bs/to-byte-array body)
          ^ExportTraceServiceRequest service-request (ExportTraceServiceRequest/parseFrom body-bytes)
          resources-spans (traces/service-request->events service-request)]
      (doseq [resource-spans resources-spans]
        (doseq [scope-spans resource-spans]
          (doseq [event scope-spans]
            (stream/push! stream-handler event stream-name))))
      {:status 200}))
  (not-found [_ _]
    {:status 404
     :body {:error "not found"}})
  (metrics [_ _]
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body (.getBytes ^String (metric/scrape registry))}))

(def path-vars-regex #"[a-zA-Z0-9~._+~-]+")

(def router
  [["/api/v1/stream" {:get {:handler list-streams}}]
   ["/api/v1/stream/:name" {:put {:handler push-event
                                  :spec :mirabelle.http.stream/event}
                            :post {:handler add-stream
                                   :spec :mirabelle.http.stream/add}
                            :get {:handler get-stream
                                  :spec :mirabelle.http.stream/get}
                            :delete {:handler remove-stream
                                     :spec :mirabelle.http.stream/remove}}]
   ["/api/v1/fluentbit/:name" {:post {:handler fluentbit
                                      :spec any?}}]
   ["/api/v1/prometheus/remote-write/:name" {:post {:handler prom-remote-write
                                                    :spec :mirabelle.http.prometheus/remote-write}}]
   ["/api/v1/opentelemetry/v1/traces/:name" {:post {:handler otel-traces
                                                    :spec :mirabelle.http.prometheus/remote-write}}]
   ["/metrics" {:get {:handler metrics}}]
   ["/healthz" {:get {:handler healthz}}]
   ["/health" {:get {:handler healthz}}]])

(defn assert-spec-valid
  [spec params]
  (if spec
    (ex/assert-spec-valid spec params)
    params))
