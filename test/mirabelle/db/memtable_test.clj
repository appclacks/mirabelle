(ns mirabelle.db.memtable-test
  (:require [clojure.test :refer :all]
            [com.stuartsierra.component :as component]
            [mirabelle.db.memtable :as memtable])
  (:import fr.mcorbin.mirabelle.memtable.Serie))

(deftest event->serie-test
  (is (= (Serie. "foo"
                 (java.util.HashMap. {"host" "bar"}))
         (memtable/event->serie {:service "foo" :host "bar"}
                                [:host])))
  (is (= (Serie. "foo"
                 (java.util.HashMap. {"host" "bar" "environment" "prod"}))
         (memtable/event->serie {:service "foo" :host "bar" :environment "prod"}
                                [:host :environment]))))

(deftest ->serie-test
  (is (= (Serie. "foo"
                 (java.util.HashMap. {"host" "bar"}))
         (memtable/->serie "foo"
                           {:host "bar"})))
  (is (= (Serie. "foo"
                 (java.util.HashMap. {"host" "bar" "environment" "prod"}))
         (memtable/->serie "foo"
                           {:host "bar" :environment "prod"}))))

(deftest memtable-engine-test
  (let [engine (component/start (memtable/map->MemtableEngine
                                 {:memtable-max-ttl 60
                                  :memtable-cleanup-duration 20}))]
    (is (nil? (memtable/values engine "bar" {:host "1"})))
    (memtable/inject! engine
                      {:host 1 :service "bar" :time 1}
                      [:host])
    (is (= [{:host 1 :service "bar" :time 1}]
           (memtable/values engine "bar" {:host "1"})))
    (memtable/inject! engine
                      {:host 1 :service "bar" :time 50}
                      [:host])
    (is (= [{:host 1 :service "bar" :time 1}
            {:host 1 :service "bar" :time 50}]
           (memtable/values engine "bar" {:host "1"})))
    (memtable/inject! engine
                      {:host 1 :service "bar" :time 62}
                      [:host])
    (is (= [{:host 1 :service "bar" :time 50}
            {:host 1 :service "bar" :time 62}]
           (memtable/values engine "bar" {:host "1"})))
    (is (= [{:host 1 :service "bar" :time 50}]
           (memtable/values-in engine "bar" {:host "1"} 49 51)))
    (memtable/inject! engine
                      {:host 1 :service "another" :time 1}
                      [:host])
    (is (= [{:host 1 :service "another" :time 1}]
           (memtable/values engine "another" {:host "1"})))
    (memtable/remove-serie engine "bar" {:host 1})
    (is (nil? (memtable/values engine "bar" {:host "1"})))))
