(ns mirabelle.handler
  (:require [clojure.edn :as edn]
            [corbihttp.metric :as metric]
            [mirabelle.db.memtable :as memtable]
            [mirabelle.stream :as stream])
  (:import java.util.Base64))

(defprotocol IHandler
  (get-serie [this request] "Get a serie")
  (healthz [this request] "Healthz handler")
  (add-stream [this request] "add a new stream")
  (remove-stream [this request] "remove a stream")
  (list-streams [this request] "list dynamic streams")
  (not-found [this request] "Not found handler")
  (metrics [this request] "Return the metrics"))

(defn get-serie-values
  [memtable-engine request]
  (let [params (:query-params request)
        service (:service (:route-params request))
        from (:from params)
        to (:to params)
        labels (dissoc params :from :to)]
    (if (and from to)
      (memtable/values-in memtable-engine service labels from to)
      (memtable/values memtable-engine service labels))))

(defn from-base64
  [s]
  (String. #^bytes (.decode (Base64/getDecoder) ^String s)))

(defrecord Handler [memtable-engine
                    stream-handler
                    registry]
  IHandler
  (get-serie [this request]
    {:status 200
     :body (or (get-serie-values memtable-engine request) [])})
  (healthz [this request]
    {:status 200
     :body {:message "ok"}})
  (add-stream [this request]
    (let [stream-name (:stream-name (:route-params request))
          config (-> request :body :config from-base64 edn/read-string)]
      (stream/add-dynamic-stream stream-handler stream-name config)
      {:status 200
       :body {:message "stream added"}}))
  (remove-stream [this request]
    (let [stream-name (:stream-name (:route-params request))]
      (stream/remove-dynamic-stream stream-handler stream-name)
      {:status 200
       :body {:message "stream removed"}}))
  (list-streams [this request]
    {:status 200
     :body (stream/list-dynamic-streams stream-handler)})
  (not-found [this request]
    {:status 404
     :body {:error "not found"}})
  (metrics [this request]
    {:status 200
     :headers {"Content-Type" "text/plain"}
     :body (.getBytes (metric/scrape registry))}))
