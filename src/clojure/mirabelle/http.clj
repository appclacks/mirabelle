(ns mirabelle.http
  (:require [corbihttp.interceptor.auth :as itc-auth]
            [corbihttp.interceptor.error :as itc-error]
            [corbihttp.interceptor.handler :as itc-handler]
            [corbihttp.interceptor.id :as itc-id]
            [corbihttp.interceptor.json :as itc-json]
            [corbihttp.interceptor.metric :as itc-metric]
            [corbihttp.interceptor.ring :as itc-ring]
            [corbihttp.interceptor.response :as itc-response]
            [corbihttp.interceptor.route :as itc-route]
            [corbihttp.metric :as metric]
            [exoscale.interceptor :as itc]
            [mirabelle.handler :as mh]
            [mirabelle.transport.websocket :as ws]
            [ring.adapter.jetty9 :as jetty]
            [ring.websocket :as ringws]))

(defn ws-interceptor
  [pubsub nb-conn]
  {:name :websocket
   :enter (fn [ctx]
            (if (jetty/ws-upgrade-request? (:request ctx))
              (itc/halt (ws/websocket-handler (:request ctx) pubsub nb-conn))
              ctx))})

(defn debug-interceptor
  [string]
  {:name :websocket
   :enter (fn [ctx]
            (println (System/nanoTime) " " string)
            ctx)})

(defn interceptor-chain
  [{:keys [api-handler registry config pubsub]}]
  (let [nb-conn (atom 0)]
    (metric/gauge! registry
                   :websocket.connections.count
                   {}
                   (fn [] @nb-conn))
    [itc-response/response                  ;;leave
     (itc-error/last-error registry)        ;;error
     (itc-metric/response-metrics registry) ;; leave
     itc-json/json                          ;; leave
     itc-error/error                        ;; error
     (when-let [basic-auth (:basic-auth config)]
       (itc-auth/basic-auth basic-auth))
     (ws-interceptor pubsub nb-conn)        ;; enter
     (itc-route/route {:router mh/router
                       :registry registry
                       :handler-component api-handler}) ;; enter
     itc-id/request-id                                  ;;enter
     itc-ring/params         ;; enter
     itc-ring/keyword-params ;; enter
     itc-json/request-params ;; enter
     (itc-handler/main-handler {:registry registry
                                :handler-component api-handler})]))
