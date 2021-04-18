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
  (search-index [this request] "query the index")
  (list-streams [this request] "list dynamic streams")
  (push-event [this request] "Push an event to a stream")
  (current-time [this request] "get the current time of the real time engine.")
  (not-found [this request] "Not found handler")
  (metrics [this request] "Return the metrics"))

(defrecord Handler [stream-handler
                    registry]
  IHandler
  (healthz [this request]
    {:status 200
     :body {:message "ok"}})
  (search-index [this request]
    (let [query (-> request :body :query b64/from-base64 edn/read-string)]
      {:status 200
       :body {:events (-> (stream/context stream-handler :streaming)
                           :index
                           (index/search query))}}))
  (add-stream [this request]
    (let [stream-name (:stream-name (:route-params request))
          config (-> request :body :config b64/from-base64 edn/read-string)]
      (stream/add-dynamic-stream stream-handler stream-name config)
      {:status 200
       :body {:message "stream added"}}))
  (push-event [this request]
    ;; todo move coercion somewhere else
    (let [stream-name (keyword (:stream-name (:route-params request)))]
      (stream/push! stream-handler (:body request) stream-name)
      {:status 200
       :body {:message "ok"}}))
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
  (current-time [this request]
    {:status 200
     :body {:current-time (-> (stream/context stream-handler :streaming)
                              :index
                              index/current-time)}})
  (not-found [this request]
    {:status 404
     :body {:error "not found"}})
  (metrics [this request]
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body (.getBytes ^String (metric/scrape registry))}))
