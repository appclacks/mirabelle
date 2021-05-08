;; This code is heavily inspired from the Riemann code base
;; Copyright Riemann authors (riemann.io), thanks to them!
(ns mirabelle.transport.websocket
  (:require [bidi.bidi :as bidi]
            [cheshire.core :as json]
            [com.stuartsierra.component :as component]
            [corbihttp.log :as log]
            [corbihttp.metric :as metric]
            [clj-http.util :as clj-http]
            [clojure.edn :as edn]
            [clojure.string :as string]
            [mirabelle.action.condition :as condition]
            [mirabelle.index :as index]
            [mirabelle.pubsub :as pubsub]
            [mirabelle.b64 :as b64]
            [org.httpkit.server :as http]))

(defn http-query-map
  "Converts a URL query string into a map."
  [string]
  (apply hash-map
         (map clj-http/url-decode
              (mapcat (fn [kv] (string/split kv #"=" 2))
                      (string/split string #"&")))))

(defn request->pred
  "Returns the predicate for the channel events based on the request query params"
  [query-params]
  (-> query-params
      (get "query")
      b64/from-base64
      edn/read-string
      condition/compile-conditions))

(defn ws-handler
  [pubsub actions ch pred channel]
  (if-not (http/websocket? ch)
    (log/info {} "Ignoring non-websocket request to websocket server.")
    (let [handler (fn emit [event]
                    (when (pred event)
                      (http/send! ch (json/generate-string event))))
          sub-id (pubsub/add pubsub channel handler)]
      (log/info {} (format "New websocket subscription %s %s" channel sub-id))

      ;; When the channel closes, unsubscribe.
      (swap! actions conj
             (fn []
               (log/info {} (format "Closing websocket subscription %s %s"
                                    channel sub-id))
               (pubsub/rm pubsub channel sub-id)))

      ; Close channel on nil msg
      (http/on-receive ch (fn [data] (when-not data (http/close ch)))))))

(def routes
  ["/"
   [[#"index/?" :ws/index]
    [["channel/" :channel #"/?"] :ws/channel]
    [true :ws/not-found]]])

(defn handler
  [pubsub registry]
  (let [nb-conn (atom 0)]
    (metric/gauge! registry
                   :websocket.connection.count
                   {}
                   (fn [] @nb-conn))
    (fn handle [request]
      (http/with-channel request ch
        (when (http/websocket? ch)
          (try
            (let [actions (atom [])
                  uri (:uri request)
                  request (bidi/match-route* routes uri request)
                  query-params (->(:query-string request)
                                  http-query-map)
                  pred (request->pred query-params)]
              (if (= :ws/not-found (:handler request))
                (do
                  (log/info {} "Unknown URI " (:uri request) ", closing")
                  (http/close ch))
                (do
                  (swap! nb-conn inc)
                  (http/on-close ch
                                 (fn [_]
                                   (swap! nb-conn dec)
                                   (doseq [action @actions]
                                     (action))))
                  (condp = (:handler request)
                    :ws/index (ws-handler pubsub
                                          actions
                                          ch
                                          pred
                                          (index/channel (get query-params
                                                              "stream"
                                                              :default)))
                    :ws/channel (ws-handler pubsub
                                            actions
                                            ch
                                            pred
                                            (-> request :route-params :channel keyword))))))

            (catch Exception e
              (log/error {} e "Error in the websocket handler")
              (http/close ch))))))))

(defrecord WebsocketServer [host port server pubsub registry]
  component/Lifecycle
  (start [this]
    (assoc this :server (http/run-server
                         (handler pubsub registry)
                         {:ip host :port port})))
  (stop [this]
    (when server
      (try
        (server)
        (catch Exception e
          (log/error {} e))))))
