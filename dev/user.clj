(ns user
  (:require [clojure.tools.namespace.repl :refer [refresh]]
            [clj-memory-meter.core :as meter]
            [mirabelle.core :as core]))

(defn start!
  []
  (core/start!)
  "started")

(defn stop!
  []
  (core/stop!)
  "stopped")

(defn restart!
  []
  (stop!)
  (refresh)
  (start!))

(defn reload!
  []
  (core/reload!)
  "reloaded")

(comment
  (require '[riemann.codec :as c])
  (require '[clj-memory-meter.core :as mm])
  (require '[clojure.data.fressian :as fress])
  (mm/measure [(c/map->Event
                {:host "foo" :service "bar" :metric 10 :time 1 :ttl 60 :tags ["prod" "db"] :description "blablabla eaz az azr za aza zaz aze aze azazeaaz az az" :state "ok" :env "prod" :location "gv2" :foo "bar"})
               (c/map->Event
                {:host "foo" :service "bar" :metric 10 :time 1 :ttl 60 :tags ["prod" "db"] :description "blablabla eaz az azr za aza zaz aze aze azazeaaz az az" :state "ok" :env "prod" :location "gv2" :foo "bar"})
               (c/map->Event
                {:host "foo" :service "bar" :metric 10 :time 1 :ttl 60 :tags ["prod" "db"] :description "blablabla eaz az azr za aza zaz aze aze azazeaaz az az" :state "ok" :env "prod" :location "gv2" :foo "bar"})
               (c/map->Event
                {:host "foo" :service "bar" :metric 10 :time 1 :ttl 60 :tags ["prod" "db"] :description "blablabla eaz az azr za aza zaz aze aze azazeaaz az az" :state "ok" :env "prod" :location "gv2" :foo "bar"})
               (c/map->Event
                {:host "foo" :service "bar" :metric 10 :time 1 :ttl 60 :tags ["prod" "db"] :description "blablabla eaz az azr za aza zaz aze aze azazeaaz az az" :state "ok" :env "prod" :location "gv2" :foo "bar"})
               (c/map->Event
                {:host "foo" :service "bar" :metric 10 :time 1 :ttl 60 :tags ["prod" "db"] :description "blablabla eaz az azr za aza zaz aze aze azazeaaz az az" :state "ok" :env "prod" :location "gv2" :foo "bar"})
               (c/map->Event
                {:host "foo" :service "bar" :metric 10 :time 1 :ttl 60 :tags ["prod" "db"] :description "blablabla eaz az azr za aza zaz aze aze azazeaaz az az" :state "ok" :env "prod" :location "gv2" :foo "bar"})]))



