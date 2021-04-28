(ns mirabelle.handler
  (:require [clojure.edn :as edn]
            [corbihttp.metric :as metric]
            [exoscale.coax :as c]
            [exoscale.ex :as ex]
            [mirabelle.b64 :as b64]
            [mirabelle.index :as index]
            [mirabelle.stream :as stream]))

(defprotocol IHandler
  (healthz [this params] "Healthz handler")
  (add-stream [this params] "add a new stream")
  (get-stream [this params] "Get a stream")
  (remove-stream [this params] "remove a stream")
  (search-index [this params] "query the index")
  (list-streams [this params] "list dynamic streams")
  (push-event [this params] "Push an event to a stream")
  (current-time [this params] "get the current time of the real time engine.")
  (not-found [this params] "Not found handler")
  (metrics [this params] "Return the metrics"))

(defrecord Handler [stream-handler
                    registry]
  IHandler
  (healthz [_ _]
    {:status 200
     :body {:message "ok"}})
  (search-index [_ {:keys [params]}]
    (let [query (-> params :query b64/from-base64 edn/read-string)
          stream-name (:name params)
          compiled-stream (-> (stream/get-dynamic-stream stream-handler stream-name))]
      {:status 200
       :body {:events (-> compiled-stream
                          :context
                          :index
                          (index/search query))}}))
  (add-stream [_ {:keys [params]}]
    (let [stream-name (:name params)
          config (-> params :config b64/from-base64 edn/read-string)]
      (stream/add-dynamic-stream stream-handler stream-name config)
      {:status 200
       :body {:message "stream added"}}))
  (push-event [_ {:keys [params]}]
    (let [stream-name (:name params)]
      (stream/push! stream-handler (:event params) stream-name)
      {:status 200
       :body {:message "ok"}}))
  (remove-stream [_ {:keys [params]}]
    (let [stream-name (:name params)]
      (stream/remove-dynamic-stream stream-handler stream-name)
      {:status 200
       :body {:message "stream removed"}}))
  (get-stream [_ {:keys [params]}]
    (let [stream-name (:name params)
          stream (stream/get-dynamic-stream stream-handler stream-name)
          config (-> stream
                     (dissoc :context :entrypoint)
                     pr-str
                     b64/to-base64)
          index (get-in stream [:context :index])]
      {:status 200
       :body {:config config
              :current-time (index/current-time index)}}))
  (list-streams [_ _]
    {:status 200
     :body {:streams (stream/list-dynamic-streams stream-handler)}})
  (current-time [_ _]
    {:status 200
     :body {:current-time (-> (stream/context stream-handler :streaming)
                              :index
                              index/current-time)}})
  (not-found [_ _]
    {:status 404
     :body {:error "not found"}})
  (metrics [_ _]
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body (.getBytes ^String (metric/scrape registry))}))

(def dispatch-map
  {:index/search {:handler-fn search-index
                  :spec :mirabelle.http.index/search}
   :stream/list {:handler-fn list-streams}
   :stream/event {:handler-fn push-event
                  :spec :mirabelle.http.stream/event}
   :stream/add {:handler-fn add-stream
                :spec :mirabelle.http.stream/add}
   :stream/remove {:handler-fn remove-stream
                   :spec :mirabelle.http.stream/remove}
   :stream/current-time {:handler-fn current-time}
   :stream/get {:handler-fn get-stream
                :spec :mirabelle.http.stream/get}
   :system/metrics {:handler-fn metrics}
   :system/healthz {:handler-fn healthz}
   :system/not-found {:handler-fn not-found}})

(defn assert-spec-valid
  [spec params]
  (if spec
    (ex/assert-spec-valid spec params)
    params))

(defn handle
  [request handler registry]
  (let [req-handler (:handler request)]
    (if-let [{:keys [handler-fn spec]} (get dispatch-map req-handler)]
      (metric/with-time
        registry
        :http.request.duration
        {"uri" (str (:uri request))
         "method"  (-> request :request-method name)}
        (->> (c/coerce spec (:all-params request {}))
             (assert-spec-valid spec)
             (hash-map :params)
             (handler-fn handler)))
      (throw (ex/ex-fault (format "unknown handler %s" req-handler)
                          {:handler req-handler})))))
