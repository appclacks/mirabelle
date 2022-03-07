;; This code is heavily inspired from the Riemann code base
;; Copyright Riemann authors (riemann.io), thanks to them!
(ns mirabelle.transport.websocket
  (:require [cheshire.core :as json]
            [corbihttp.log :as log]
            [clj-http.util :as clj-http]
            [clojure.edn :as edn]
            [clojure.string :as string]
            [exoscale.ex :as ex]
            [mirabelle.action.condition :as condition]
            [mirabelle.index :as index]
            [mirabelle.pubsub :as pubsub]
            [mirabelle.b64 :as b64]
            [reitit.core :as r]
            [ring.adapter.jetty9.websocket :as websocket])
  (:import org.eclipse.jetty.websocket.api.WebSocketAdapter))

(defn http-query-map
  "Converts a URL query string into a map."
  [string]
  (apply hash-map
         (map clj-http/url-decode
              (mapcat (fn [kv] (string/split kv #"=" 2))
                      (string/split string #"&")))))

(defn query-true?
  "Helper to support the Riemann default `true` query."
  [query]
  (if (= "true" query)
    (b64/to-base64 "[:always-true]")
    query))

(defn request->pred
  "Returns the predicate for the channel events based on the request query params"
  [query-params]
  (let [query (get query-params "query")]
    (try
      (-> query
          query-true?
          b64/from-base64
          edn/read-string
          condition/compile-conditions)
      (catch Exception e
        (ex/ex-incorrect! (format "Invalid websocket query %s (base64)" query)
                          (ex-data e)
                          e)))))

(def router
  (r/router
   [["/index" :ws/index]
    ["/channel/:channel" :ws/channel]]))

(defn ws-handler
  [ps actions ws pred channel]
  (let [handler (fn emit [event]
                  (when (pred event)
                    (websocket/send! ws (.getBytes (json/generate-string event)))))
        sub-id (pubsub/add ps channel handler)]
    (log/info {} (format "New websocket subscription %s %s" channel sub-id))

    (swap! actions conj
           (fn []
             (log/info {} (format "Closing websocket subscription %s %s"
                                  channel sub-id))
             (pubsub/rm ps channel sub-id)))))

(defn websocket-handler [pubsub nb-conn]
  (fn [upgrade-request]
    (let [provided-subprotocols (:websocket-subprotocols upgrade-request)
          provided-extensions (:websocket-extensions upgrade-request)
          actions (atom [])]
      {:on-connect (fn on-connect [ws]
                     (let [request (websocket/req-of ws)
                           uri (:uri request)
                           route-match (r/match-by-path router uri)
                           handler (or (-> route-match :data :name)
                                       :ws/not-found)
                           query-params (-> (:query-string request)
                                            http-query-map)
                           pred (request->pred query-params)]
                       (if (= :ws/not-found handler)
                         (do
                           (log/info {} "Unknown URI " (:uri request) ", closing websocket connection")
                           (websocket/close! ws))
                         (do
                           (swap! nb-conn inc)
                           (condp = handler
                             :ws/index (ws-handler pubsub
                                                   actions
                                                   ws
                                                   pred
                                                   (index/channel (get query-params
                                                                       "stream"
                                                                       :default)))
                             :ws/channel (ws-handler pubsub
                                                     actions
                                                     ws
                                                     pred
                                                     (-> route-match :path-params :channel keyword)))))))
       :on-close (fn on-close [_ status-code reason]
                   (log/info {} (format "Closing Websocket connection status=%d reason=%s" status-code reason))
                   (swap! nb-conn dec)
                   (doseq [action @actions]
                     (action)))
       :on-error (fn on-error [ws e]
                   (log/error {} e "Error in the websocket handler")
                   (websocket/close! ws))
       :on-ping (fn on-ping [^WebSocketAdapter ws payload]
                  (-> ws .getRemote (.sendPong payload)))
       :subprotocol (first provided-subprotocols)
       :extentions provided-extensions})))
