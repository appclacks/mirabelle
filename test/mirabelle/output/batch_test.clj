(ns mirabelle.output.batch-test
  (:require [clojure.test :refer :all]
            [com.stuartsierra.component :as component]
            [mirabelle.output.batch :as batch]
            [mirabelle.time :as time]))

(deftest batch-size-test
  (let [result (atom [])
        downstream (fn [events] (swap! result conj events))
        batcher (component/start
                 (batch/map->SimpleBatcher
                  {:max-duration-ns (time/s->ns 100000)
                   :max-size 4
                   :downstream downstream
                   }))]
    (batch/inject! batcher {:a "1"})
    (is (= [] @result))
    (batch/inject! batcher {:a "2"})
    (is (= [] @result))
    (batch/inject! batcher {:a "3"})
    (is (= [] @result))
    (batch/inject! batcher {:a "4"})
    (is (= [[{:a "1"} {:a "2"} {:a "3"} {:a "4"}]] @result))
    (batch/inject! batcher {:a "5"})
    (is (= (count @result) 1))
    (batch/inject! batcher {:a "6"})
    (is (= (count @result) 1))
    (batch/inject! batcher {:a "7"})
    (is (= (count @result) 1))
    (batch/inject! batcher {:a "8"})
    (is (= (count @result) 2))
    (is (= [[{:a "1"} {:a "2"} {:a "3"} {:a "4"}]
            [{:a "5"} {:a "6"} {:a "7"} {:a "8"}]]
           @result))
    (batch/inject! batcher {:a "9"})
    (is (= [[{:a "1"} {:a "2"} {:a "3"} {:a "4"}]
            [{:a "5"} {:a "6"} {:a "7"} {:a "8"}]]
           @result))
    (component/stop batcher)
    (is (= [[{:a "1"} {:a "2"} {:a "3"} {:a "4"}]
            [{:a "5"} {:a "6"} {:a "7"} {:a "8"}]
            [{:a "9"}]]
           @result))))

(deftest batch-duration-test
  (let [result (atom [])
        downstream (fn [events] (swap! result conj events))
        batcher (component/start
                 (batch/map->SimpleBatcher
                  {:max-duration-ns (time/s->ns 0.5)
                   :max-size 1000
                   :downstream downstream}))]
    (batch/inject! batcher {:a "1"})
    (is (= [] @result))
    (batch/inject! batcher {:a "2"})
    (is (= [] @result))
    (Thread/sleep 100)
    (is (= [] @result))
    (Thread/sleep 600)
    (is (= [[{:a "1"} {:a "2"}]] @result))
    (Thread/sleep 600)
    (is (= [[{:a "1"} {:a "2"}]] @result))
    (component/stop batcher)))
