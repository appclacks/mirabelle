(ns mirabelle.interceptor.route
  (:require [bidi.bidi :as bidi]
            [exoscale.ex :as ex]
            [mirabelle.handler :as handler]
            [corbihttp.metric :as metric]))

(def v1
  {[#"/serie/" :service #"/?"] {:get :serie/get}
   [#"/stream/" :stream-name #"/?"] {:post :stream/add}
   [#"/stream/" :stream-name #"/?"] {:put :stream/event}
   [#"/stream/" :stream-name #"/?"] {:delete :stream/remove}
   [#"/index/search/?"] {:post :index/search}
   [#"/stream/" :stream-name #"/?"] {:get :stream/get}
   [#"/current-time/?"] {:get :stream/current-time}
   [#"/stream/?"] {:get :stream/list}})

(def routes
  ["/"
   [["api/v1" v1]
    [#"metrics/?" :system/metrics]
    [#"healthz/?" :system/healthz]
    [#"health/?" :system/healthz]
    [true :system/not-found]]])

(defn route!
  [request handler registry]
  (metric/with-time
    registry
    :http.request.duration
    {"uri" (str (:uri request))
     "method"  (str (some-> request
                            :request-method
                            name))}
    (let [req-handler (:handler request)]
      (condp = req-handler
        :serie/get (handler/get-serie handler request)
        :index/search (handler/search-index handler request)
        :stream/list (handler/list-streams handler request)
        :stream/event (handler/push-event handler request)
        :stream/add (handler/add-stream handler request)
        :stream/remove (handler/remove-stream handler request)
        :stream/current-time (handler/current-time handler request)
        :stream/get (handler/get-stream handler request)
        :system/metrics (handler/metrics handler request)
        :system/healthz (handler/healthz handler request)
        :system/not-found (handler/not-found handler request)
        (throw (ex/ex-fault (format "unknown handler %s" req-handler)
                            {:handler req-handler}))))))

(defn route
  [handler registry]
  {:name ::route
   :enter
   (fn [{:keys [request] :as ctx}]
     (assoc ctx :response (route! request handler registry)))})

(def match-route
  {:name ::match-route
   :enter
   (fn [{:keys [request] :as ctx}]
     (let [uri (:uri request)
           request (bidi/match-route* routes uri request)]
       (when (= :system/not-found (:handler request))
         (throw (ex/ex-info
                 "Not found"
                 [::not-found [:corbi/user ::ex/not-found]])))
       (assoc ctx :request request)))})
