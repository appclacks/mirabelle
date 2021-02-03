(ns mirabelle.stream-test
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.test :refer :all]
            [mirabelle.action :as a]
            [mirabelle.io.file :as io-file]
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

(deftest above-dt-test
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

(deftest between-dt-test
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

(deftest outside-dt-test
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

(deftest critical-dt-test
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

(deftest ddt-test
  (let [recorder (atom [])
        stream {:name "my-stream"
                :description "foo"
                :actions (a/ddt
                          (a/test-action recorder))}
        {:keys [entrypoint]} (stream/compile-stream! {} stream)]
    (entrypoint {:metric 1 :time 1})
    (is (= [] @recorder))
    (entrypoint {:metric 4 :time 2})
    (entrypoint {:metric 0 :time 12})
    (is (= [{:metric 3 :time 2}
            {:metric (/ -4 10) :time 12}] @recorder))))

(deftest ddt-pos-test
  (let [recorder (atom [])
        stream {:name "my-stream"
                :description "foo"
                :actions (a/ddt-pos
                          (a/test-action recorder))}
        {:keys [entrypoint]} (stream/compile-stream! {} stream)]
    (entrypoint {:metric 1 :time 1})
    (is (= [] @recorder))
    (entrypoint {:metric 4 :time 2})
    (entrypoint {:metric 0 :time 12})
    (entrypoint {:metric 2 :time 14})
    (is (= [{:metric 3 :time 2}
            {:metric 1 :time 14}] @recorder))))

(deftest with-test-stream
  (let [recorder (atom [])
        stream {:name "my-stream"
                :description "foo"
                :actions (a/with {:foo 1 :metric 2}
                          (a/test-action recorder))}
        {:keys [entrypoint]} (stream/compile-stream! {} stream)]
    (entrypoint {:metric 1 :time 1})
    (is (= [{:metric 2 :time 1 :foo 1}] @recorder)))
  (let [recorder (atom [])
        stream {:name "my-stream"
                :description "foo"
                :actions (a/with :metric 2
                          (a/test-action recorder))}
        {:keys [entrypoint]} (stream/compile-stream! {} stream)]
    (entrypoint {:metric 1 :time 1})
    (is (= [{:metric 2 :time 1}] @recorder))))

(deftest io-file-test
  (let [stream {:name "my-stream"
                :description "foo"
                :actions (a/critical
                          (a/push-io! :file-example-io))}
        file (io/file "file-example-io")
        io-component (io-file/map->FileIO {:path (.getPath file)})
        {:keys [entrypoint]} (stream/compile-stream!
                              {:io {:file-example-io io-component}}
                              stream)]
    (entrypoint {:state "critical" :time 1})
    (entrypoint {:state "critical" :time 2})
    (entrypoint {:state "ok" :time 2})
    (let [result (slurp file)]
      (is ["{:state \"critical\" :time 1}"
           "{:state \"critical\" :time 2}"]
       (string/split result #"\n")))))

(deftest full-test
  (let [stream {:name "my-stream"
                :description "foo"
                :actions (a/sdo
                          (a/above-dt 10 20)
                          (a/between-dt 10 20 30)
                          (a/decrement)
                          (a/throttle 10)
                          (a/split
                           [:> :metric 10] (a/critical))
                          (a/critical)
                          (a/critical-dt 10)
                          (a/debug)
                          (a/info)
                          (a/error)
                          (a/expired)
                          (a/fixed-event-window 3
                                                (a/list-mean
                                                 (a/sflatten))
                                                (a/list-max)
                                                (a/list-min)
                                                (a/list-rate))
                          (a/increment)
                          (a/not-expired)
                          (a/tag "foo")
                          (a/ddt)
                          (a/ddt-pos)
                          (a/untag "foo")
                          (a/tag ["foo" "bar"])
                          (a/untag ["foo" "bar"])
                          (a/outside-dt 2 10 20)
                          (a/coalesce 2 [:host])
                          (a/scale 100)
                          (a/with :foo 1)
                          (a/with {:foo 1})
                          (a/where [:> :metric 10])
                          (a/where [:and
                                    [:< :metric 10]
                                    [:> :metric 1]]))}
        {:keys [entrypoint]} (stream/compile-stream! {} stream)]
    (is (fn? entrypoint))
    (entrypoint {:state "ok" :time 2 :metric 1 :host "foo"})
    (entrypoint {:state "ok" :time 4 :metric 1 :host "foo"})
    (entrypoint {:state "ok" :time 100 :metric 1 :host "foo"})
    (entrypoint {:state "ok" :time 200 :metric 1 :host "foo"})))

(deftest split-test
  (let [recorder (atom [])
        recorder2 (atom [])
        recorder3 (atom [])
        stream {:name "my-stream"
                :description "foo"
                :actions (a/split
                          [:> :metric 10] (a/test-action recorder)
                          [:> :metric 5] (a/test-action recorder2)
                          (a/test-action recorder3))}
        {:keys [entrypoint]} (stream/compile-stream! {} stream)]
    (entrypoint {:metric 11 :time 1})
    (entrypoint {:metric 6 :time 2})
    (entrypoint {:metric 12 :time 3})
    (entrypoint {:metric 1 :time 4})
    (entrypoint {:metric 2 :time 5})
    (is (= [{:metric 11 :time 1}
            {:metric 12 :time 3}]
           @recorder))
    (is (= [{:metric 6 :time 2}]
           @recorder2))
    (is (= [{:metric 1 :time 4}
            {:metric 2 :time 5}]
           @recorder3)))
  (let [recorder (atom [])
        recorder2 (atom [])
        stream {:name "my-stream"
                :description "foo"
                :actions (a/split
                          [:> :metric 10] (a/test-action recorder)
                          [:> :metric 5] (a/test-action recorder2))}
        {:keys [entrypoint]} (stream/compile-stream! {} stream)]
    (entrypoint {:metric 11 :time 1})
    (entrypoint {:metric 6 :time 2})
    (entrypoint {:metric 12 :time 3})
    (entrypoint {:metric 1 :time 4})
    (entrypoint {:metric 2 :time 5})
    (is (= [{:metric 11 :time 1}
            {:metric 12 :time 3}]
           @recorder))
    (is (= [{:metric 6 :time 2}]
           @recorder2)))
  (let [recorder (atom [])
        stream {:name "my-stream"
                :description "foo"
                :actions (a/split
                          [:> :metric 10] (a/test-action recorder))}
        {:keys [entrypoint]} (stream/compile-stream! {} stream)]
    (entrypoint {:metric 11 :time 1})
    (entrypoint {:metric 6 :time 2})
    (entrypoint {:metric 12 :time 3})
    (entrypoint {:metric 1 :time 4})
    (entrypoint {:metric 2 :time 5})
    (is (= [{:metric 11 :time 1}
            {:metric 12 :time 3}]
           @recorder))))
