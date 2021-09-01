(ns mirabelle.http
  (:require [corbihttp.interceptor.error :as itc-error]
            [corbihttp.interceptor.handler :as itc-handler]
            [corbihttp.interceptor.id :as itc-id]
            [corbihttp.interceptor.json :as itc-json]
            [corbihttp.interceptor.metric :as itc-metric]
            [corbihttp.interceptor.ring :as itc-ring]
            [corbihttp.interceptor.response :as itc-response]
            [corbihttp.interceptor.route :as itc-route]
            [mirabelle.handler :as mh]))

(defn interceptor-chain
  [{:keys [api-handler registry]}]
  [itc-response/response ;;leave
   (itc-error/last-error registry) ;;error
   (itc-metric/response-metrics registry) ;; leave
   itc-json/json ;; leave
   itc-error/error ;; error
   (itc-route/route {:dispatch-map mh/dispatch-map
                     :registry registry
                     :handler-component api-handler
                     :not-found-handler mh/not-found}) ;; enter
   itc-id/request-id ;;enter
   itc-ring/cookies ;; enter + leave
   itc-ring/params ;; enter
   itc-ring/keyword-params ;; enter
   itc-json/request-params ;; enter
   (itc-handler/main-handler {:dispatch-map mh/dispatch-map
                              :registry registry
                              :handler-component api-handler})])
