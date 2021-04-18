(ns mirabelle.http
  (:require [com.stuartsierra.component :as component]
            [corbihttp.interceptor.error :as itc-error]
            [corbihttp.interceptor.id :as itc-id]
            [corbihttp.interceptor.json :as itc-json]
            [corbihttp.interceptor.metric :as itc-metric]
            [corbihttp.interceptor.ring :as itc-ring]
            [corbihttp.interceptor.response :as itc-response]
            [exoscale.interceptor :as interceptor]
            [mirabelle.interceptor.route :as itc-route]))

(defn interceptor-chain
  [api-handler registry]
  [itc-response/response ;;leave
   (itc-error/last-error registry) ;;error
   (itc-metric/response-metrics registry) ;; leave
   itc-json/json ;; leave
   itc-error/error ;; error
   itc-id/request-id ;;enter
   itc-route/match-route ;; enter
   itc-ring/cookies ;; enter + leave
   itc-ring/params ;; enter
   itc-ring/keyword-params ;; enter
   itc-json/request-params ;; enter
   (itc-route/route api-handler registry) ;; enter
   ])

(defn execute!
  [request chain]
  (interceptor/execute {:request request} chain))
(defrecord ChainHandler [api-handler itc-chain registry]
  component/Lifecycle
  (start [this]
    (assoc this :itc-chain (interceptor-chain api-handler registry)))
  (stop [this]
    (assoc this :it-chain nil))
  clojure.lang.IFn
  (invoke [this request] (execute! request itc-chain))
  (applyTo [this args] (clojure.lang.AFn/applyToHelper this args)))
