(ns mirabelle.handler
  (:require [clojure.edn :as edn]
            [corbihttp.metric :as metric]
            [mirabelle.b64 :as b64]
            [mirabelle.index :as index]
            [mirabelle.stream :as stream]))

(defprotocol IHandler
  (get-serie [this request] "Get a serie")
  (healthz [this request] "Healthz handler")
  (add-stream [this request] "add a new stream")
  (get-stream [this request] "Get a stream")
  (remove-stream [this request] "remove a stream")
  (list-streams [this request] "list dynamic streams")
  (not-found [this request] "Not found handler")
  (metrics [this request] "Return the metrics"))

(defrecord Handler [stream-handler
                    registry]
  IHandler
  (healthz [this request]
    {:status 200
     :body {:message "ok"}})
  (add-stream [this request]
    (let [stream-name (:stream-name (:route-params request))
          config (-> request :body :config b64/from-base64 edn/read-string)]
      (stream/add-dynamic-stream stream-handler stream-name config)
      {:status 200
       :body {:message "stream added"}}))
  (remove-stream [this request]
    (let [stream-name (:stream-name (:route-params request))]
      (stream/remove-dynamic-stream stream-handler stream-name)
      {:status 200
       :body {:message "stream removed"}}))
  (get-stream [this request]
    (let [stream-name (:stream-name (:route-params request))
          stream (stream/get-dynamic-stream stream-handler stream-name)
          config (-> stream
                     (dissoc :context :entrypoint)
                     pr-str
                     b64/to-base64)
          index (get-in stream [:context :index])]
      {:status 200
       :body {:config config
              :current-time (index/current-time index)}}))
  (list-streams [this request]
    {:status 200
     :body (stream/list-dynamic-streams stream-handler)})
  (not-found [this request]
    {:status 404
     :body {:error "not found"}})
  (metrics [this request]
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body (.getBytes ^String (metric/scrape registry))}))
