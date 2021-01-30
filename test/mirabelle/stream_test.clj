(ns mirabelle.stream-test
  (:require [clojure.test :refer :all]
            [mirabelle.action :as a]
            [mirabelle.stream :as stream]))

(deftest compile!-test
  (let [recorder (atom [])
        stream {:name "my-stream"
                :description "foo"
                :actions (a/sdo
                          (a/decrement
                           (a/test-action recorder))
                          (a/where [:> :metric 10]
                                   (a/increment
                                    (a/test-action recorder))))}
        {:keys [entrypoint]} (stream/compile-stream! {} stream)]
    (is (fn? entrypoint))
    (entrypoint {:metric 12})
    (is (= [{:metric 11} {:metric 13}] @recorder))
    (entrypoint {:metric 30})
    (is (= [{:metric 11} {:metric 13} {:metric 29} {:metric 31}] @recorder))))

(deftest above-test
  (let [recorder (atom [])
        stream {:name "my-stream"
                :description "foo"
                :actions (a/above-dt 5 10
                          (a/test-action recorder))}
        {:keys [entrypoint]} (stream/compile-stream! {} stream)]
    (entrypoint {:metric 12 :time 1})
    (is (= [] @recorder))
    (entrypoint {:metric 12 :time 2})
    (entrypoint {:metric 12 :time 10})
    (entrypoint {:metric 12 :time 12})
    (is (= [{:metric 12 :time 12}] @recorder))
    (entrypoint {:metric 13 :time 14})
    (entrypoint {:metric 1 :time 15})
    (entrypoint {:metric 14 :time 20})
    (is (= [{:metric 12 :time 12} {:metric 13 :time 14}] @recorder))
    (entrypoint {:metric 15 :time 31})
    (is (= [{:metric 12 :time 12} {:metric 13 :time 14} {:metric 15 :time 31}]
           @recorder))))

(deftest between-test
  (let [recorder (atom [])
        stream {:name "my-stream"
                :description "foo"
                :actions (a/between-dt 20 30 10
                          (a/test-action recorder))}
        {:keys [entrypoint]} (stream/compile-stream! {} stream)]
    (entrypoint {:metric 21 :time 1})
    (is (= [] @recorder))
    (entrypoint {:metric 22 :time 2})
    (entrypoint {:metric 23 :time 10})
    (entrypoint {:metric 24 :time 12})
    (is (= [{:metric 24 :time 12}] @recorder))
    (entrypoint {:metric 25 :time 14})
    (entrypoint {:metric 1 :time 15})
    (entrypoint {:metric 29 :time 20})
    (is (= [{:metric 24 :time 12} {:metric 25 :time 14}] @recorder))
    (entrypoint {:metric 28 :time 31})
    (is (= [{:metric 24 :time 12} {:metric 25 :time 14} {:metric 28 :time 31}]
           @recorder))))

(deftest outside-test
  (let [recorder (atom [])
        stream {:name "my-stream"
                :description "foo"
                :actions (a/outside-dt 20 30 10
                          (a/test-action recorder))}
        {:keys [entrypoint]} (stream/compile-stream! {} stream)]
    (entrypoint {:metric 1 :time 1})
    (is (= [] @recorder))
    (entrypoint {:metric 2 :time 2})
    (entrypoint {:metric 3 :time 10})
    (entrypoint {:metric 4 :time 12})
    (is (= [{:metric 4 :time 12}] @recorder))
    (entrypoint {:metric 5 :time 14})
    (entrypoint {:metric 25 :time 15})
    (entrypoint {:metric 10 :time 20})
    (is (= [{:metric 4 :time 12} {:metric 5 :time 14}] @recorder))
    (entrypoint {:metric 40 :time 31})
    (is (= [{:metric 4 :time 12} {:metric 5 :time 14} {:metric 40 :time 31}]
           @recorder))))

(deftest critical-test
  (let [recorder (atom [])
        stream {:name "my-stream"
                :description "foo"
                :actions (a/critical-dt 10
                          (a/test-action recorder))}
        {:keys [entrypoint]} (stream/compile-stream! {} stream)]
    (entrypoint {:state "critical" :time 1})
    (is (= [] @recorder))
    (entrypoint {:state "critical" :time 2})
    (entrypoint {:state "critical" :time 10})
    (entrypoint {:state "critical" :time 12})
    (is (= [{:state "critical" :time 12}] @recorder))
    (entrypoint {:state "critical" :time 14})
    (entrypoint {:state "ok" :time 15})
    (entrypoint {:state "critical" :time 20})
    (is (= [{:state "critical" :time 12} {:state "critical" :time 14}] @recorder))
    (entrypoint {:state "critical" :time 31})
    (is (= [{:state "critical" :time 12}
            {:state "critical" :time 14}
            {:state "critical" :time 31}]
           @recorder))))
