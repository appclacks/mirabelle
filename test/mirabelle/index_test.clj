(ns mirabelle.index-test
  (:require [clojure.test :refer :all]
            [com.stuartsierra.component :as component]
            [mirabelle.index :as index]))

(deftest index-test
  (let [index (component/start (index/map->Index {}))]
    (is (= 0 (index/size-index index)))
    (is (= 0 (index/current-time index)))
    (index/insert index {:host "foo" :time 1} [:host])
    (index/insert index {:host "foo" :service "bar" :time 1 :foo "bar"} [:host :service])
    (is (= 2 (index/size-index index)))
    (is (= {:host "foo" :time 1} (index/lookup index {:host "foo"})))
    (is (= {:host "foo" :service "bar" :time 1 :foo "bar"}
           (index/lookup index {:host "foo" :service "bar"})))
    (is (nil? (index/lookup index {:host "foo" :service "baz"})))
    (is (= 0 (index/current-time index)))
    (index/new-time? index 1)
    (is (= 1 (index/current-time index)))
    (is (= 2 (index/size-index index)))
    (index/delete index {:host "foo"})
    (is (= 1 (index/size-index index)))
    (is (= {:host "foo" :service "bar" :time 1 :foo "bar"}
           (index/lookup index {:host "foo" :service "bar"})))
    (index/insert index {:host "foo" :time 1} [:host])
    (is (= #{{:host "foo" :time 1}
             {:host "foo" :service "bar" :time 1 :foo "bar"}}
           (set (index/search index [:= :host "foo"]))))
    (is (= #{{:host "foo" :service "bar" :time 1 :foo "bar"}}
           (set (index/search index [:= :service "bar"]))))
    (is (= #{{:host "foo" :service "bar" :time 1 :foo "bar"}}
           (set (index/search index [:and
                                     [:= :host "foo"]
                                     [:= :foo "bar"]]))))
    (index/new-time? index 122)
    (is (= 122 (index/current-time index)))
    (is (= #{{:host "foo" :time 1}
             {:host "foo" :service "bar" :time 1 :foo "bar"}}
           (set (index/expire index))))
    (is (= 0 (index/size-index index)))
    (is (= #{}
           (set (index/expire index))))
    (index/insert index {:host "foo" :time 110 :ttl 1} [:host])
    (index/insert index {:host "bar" :time 110 :ttl 100} [:host])
    (is (= #{{:host "foo" :time 110 :ttl 1}}
           (set (index/expire index))))
    (is (= 1 (index/size-index index)))
    (is (= {:host "bar" :time 110 :ttl 100}
           (index/lookup index {:host "bar"})))))
