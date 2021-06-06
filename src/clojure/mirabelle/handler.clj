(ns mirabelle.handler
  (:require [clojure.edn :as edn]
            [corbihttp.metric :as metric]
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
  (search-index [_ {:keys [all-params]}]
    (let [query (-> all-params :query b64/from-base64 edn/read-string)
          stream-name (:name all-params)
          index (if (= :default stream-name)
                  (:index (stream/context stream-handler :default))
                  (-> (stream/get-dynamic-stream stream-handler stream-name)
                      :context
                      :index))]
      {:status 200
       :body {:events (index/search index query)}}))
  (add-stream [_ {:keys [all-params]}]
    (let [stream-name (:name all-params)
          config (-> all-params :config b64/from-base64 edn/read-string)]
      (stream/add-dynamic-stream stream-handler stream-name config)
      {:status 200
       :body {:message "stream added"}}))
  (push-event [_ {:keys [all-params]}]
    (let [stream-name (:name all-params)]
      (stream/push! stream-handler (:event all-params) stream-name)
      {:status 200
       :body {:message "ok"}}))
  (remove-stream [_ {:keys [all-params]}]
    (let [stream-name (:name all-params)]
      (stream/remove-dynamic-stream stream-handler stream-name)
      {:status 200
       :body {:message "stream removed"}}))
  (get-stream [_ {:keys [all-params]}]
    (let [stream-name (:name all-params)
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
     :body {:current-time (-> (stream/context stream-handler :default)
                              :index
                              index/current-time)}})
  (not-found [_ _]
    {:status 404
     :body {:error "not found"}})
  (metrics [_ _]
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body (.getBytes ^String (metric/scrape registry))}))

(def path-vars-regex #"[a-zA-Z0-9~._+~-]+")

(def dispatch-map
  {:index/search {:path [#"api/v1/index/" [path-vars-regex :name] #"/search/?"]
                  :method :post
                  :handler-fn search-index
                  :spec :mirabelle.http.index/search}
   :stream/list {:path [#"api/v1/stream/?"]
                 :method :get
                 :handler-fn list-streams}
   :stream/event {:path [#"api/v1/stream/" [path-vars-regex :name] #"/?"]
                  :method :put
                  :handler-fn push-event
                  :spec :mirabelle.http.stream/event}
   :stream/add {:path [#"api/v1/stream/" [path-vars-regex :name] #"/?"]
                :method :post
                :handler-fn add-stream
                :spec :mirabelle.http.stream/add}
   :stream/remove {:path [#"api/v1/stream/" [path-vars-regex :name] #"/?"]
                   :method :delete
                   :handler-fn remove-stream
                   :spec :mirabelle.http.stream/remove}
   :stream/current-time {:path [#"api/v1/current-time/?"]
                         :method :get
                         :handler-fn current-time}
   :stream/get {:path [#"api/v1/stream/" [path-vars-regex :name] #"/?"]
                :method :get
                :handler-fn get-stream
                :spec :mirabelle.http.stream/get}
   :system/metrics {:path "metrics"
                    :method :get
                    :handler-fn metrics}
   :system/healthz {:path "healthz"
                    :method :get
                    :handler-fn healthz}})

(defn assert-spec-valid
  [spec params]
  (if spec
    (ex/assert-spec-valid spec params)
    params))
