(ns mirabelle.interceptor.route
  (:require [bidi.bidi :as bidi]
            [exoscale.ex :as ex]
            [mirabelle.handler :as handler]))

(def v1
  {[#"/stream/" :name #"/?"] {:post :stream/add}
   [#"/stream/" :name #"/?"] {:put :stream/event}
   [#"/stream/" :name #"/?"] {:delete :stream/remove}
   [#"/stream/" :name #"/?"] {:get :stream/get}
   [#"/index/" :name #"/search/?"] {:post :index/search}
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
  (handler/handle request handler registry))

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
