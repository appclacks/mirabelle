(ns mirabelle.stream-test
  (:require [clojure.java.io :as io]
            [clojure.string :as string]
            [clojure.test :refer :all]
            [corbihttp.metric :as metric]
            [mirabelle.action :as a]
            [mirabelle.io.file :as io-file]
            [mirabelle.io :refer [IO]]
            [mirabelle.pool :as pool]
            [mirabelle.stream :as stream])
  (:import mirabelle.stream.StreamHandler))

(deftest custom-action-test
  (testing "can compile with a custom action"
    (let [custom-actions {:my-custom-action 'mirabelle.action/where*}
          recorder (atom [])
          stream {:description "foo"
                  :actions {:action :my-custom-action
                            :params [[:> :metric 10]]
                            :children [{:action :test-action
                                        :params [recorder]}]}}
          {:keys [entrypoint]} (stream/compile-stream!
                                {:custom-actions custom-actions}
                                stream)]
     (is (fn? entrypoint))
     (entrypoint {:metric 12})
     (is (= [{:metric 12}] @recorder))
     (entrypoint {:metric 9})
     (is (= [{:metric 12}] @recorder))
     (entrypoint {:metric 13})
     (is (= [{:metric 12} {:metric 13}] @recorder)))))

(deftest compile!-test
  (let [recorder (atom [])
        stream {:description "foo"
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


(deftest coll-rate-test
  (let [recorder (atom [])
        stream {:name "my-stream"
                :description "foo"
                :actions
                (a/coll-rate (a/test-action recorder))}
        {:keys [entrypoint]} (stream/compile-stream! {} stream)]
    (entrypoint
     [{:host "341694922644", :service "2", :state nil, :description nil, :metric 20, :tags nil, :time 1.614428853E9, :ttl nil}
      {:host "341694922644", :service "2", :state nil, :description nil, :metric 20, :tags nil, :time 1.614428853E9, :ttl nil}
      {:host "341694922644", :service "2", :state nil, :description nil, :metric 20, :tags nil, :time 1.614428853E9, :ttl nil}
      {:host "341694922644", :service "2", :state nil, :description nil, :metric 20, :tags nil, :time 1.614428853E9, :ttl nil}
      {:host "341694922644", :service "2", :state nil, :description nil, :metric 20, :tags nil, :time 1.614428853E9, :ttl nil}])
    (is (= [{:host "341694922644", :service "2", :state nil, :description nil, :metric 100, :tags nil, :time 1.614428853E9, :ttl nil}]
           @recorder))))

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
                :actions (a/push-io! :file-example-io)}
        file "/tmp/mirabelle-test-io"
        io-component (io-file/map->FileIO {:path file})
        {:keys [entrypoint]} (stream/compile-stream!
                              {:io {:file-example-io {:component io-component}}}
                              stream)]
    (entrypoint {:state "critical" :time 1})
    (entrypoint {:state "critical" :time 1 :tags ["discard"]})
    (entrypoint [{:state "critical" :time 1 :tags ["discard"]}
                 {:state "critical" :time 1 :tags ["discard"]}
                 {:state "critical" :time 1 :tags ["ok"]}])
    (entrypoint {:state "critical" :time 2})
    (let [result (slurp file)]
      (is (= ["{:state \"critical\", :time 1}"
              "{:state \"critical\", :time 1, :tags [\"ok\"]}"
              "{:state \"critical\", :time 2}"]
             (string/split result #"\n"))))
    (io/delete-file file)))

(deftest by-test
  (testing "simple example"
    (let [recorder (atom [])
          stream {:name "my-stream"
                  :description "foo"
                  :actions (a/by [:host]
                                 (a/fixed-event-window 2
                                                       (a/test-action recorder)))}
          {:keys [entrypoint]} (stream/compile-stream! {} stream)]
      (entrypoint {:host "foo" :metric 1 :time 1})
      (entrypoint {:host "foo" :metric 2 :time 1})
      (entrypoint {:host "bar" :metric 3 :time 1})
      (entrypoint {:host "bar" :metric 4 :time 1})
      (is (= [[{:host "foo" :metric 1 :time 1}
               {:host "foo" :metric 2 :time 1}]
              [{:host "bar" :metric 3 :time 1}
               {:host "bar" :metric 4 :time 1}]]
             @recorder))
      (entrypoint {:host "bar" :metric 5 :time 2})
      (entrypoint {:host "bar" :metric 6 :time 2})
      (entrypoint {:host "baz" :metric 4 :time 1})
      (entrypoint {:host "baz" :metric 7 :time 4})
      (is (= [[{:host "foo" :metric 1 :time 1}
               {:host "foo" :metric 2 :time 1}]
              [{:host "bar" :metric 3 :time 1}
               {:host "bar" :metric 4 :time 1}]
              [{:host "bar" :metric 5 :time 2}
               {:host "bar" :metric 6 :time 2}]
              [{:host "baz" :metric 4 :time 1}
               {:host "baz" :metric 7 :time 4}]]
             @recorder)))))

(deftest full-test
  (let [stream {:name "my-stream"
                :description "foo"
                :actions (a/sdo
                          (a/above-dt 10 20)
                          (a/between-dt 10 20 30)
                          (a/decrement)
                          (a/sdissoc :foo)
                          (a/by [:host])
                          (a/by [:host :service])
                          (a/sdissoc [:host :service])
                          (a/throttle 10)
                          (a/warning)
                          (a/ewma-timeless 1)
                          (a/over 1)
                          (a/under 1)
                          (a/fixed-time-window 3)
                          (a/split
                           [:> :metric 10] (a/critical))
                          (a/critical)
                          (a/critical-dt 10)
                          (a/debug)
                          (a/info)
                          (a/error)
                          (a/expired)
                          (a/fixed-event-window 3
                                                (a/percentiles [0 0.5 1])
                                                (a/coll-mean
                                                 (a/sflatten))
                                                (a/coll-max)
                                                (a/coll-min)
                                                (a/coll-count)
                                                (a/coll-rate))
                          (a/increment)
                          (a/changed :state "ok")
                          (a/not-expired)
                          (a/tag "foo")
                          (a/ddt)
                          (a/moving-event-window 3)
                          (a/ddt-pos)
                          (a/tagged-all ["foo" "bar"])
                          (a/tagged-all "bar")
                          (a/untag "foo")
                          (a/tag ["foo" "bar"])
                          (a/untag ["foo" "bar"])
                          (a/outside-dt 2 10 20)
                          (a/coalesce 2 [:host])
                          (a/scale 100)
                          (a/with :foo 1)
                          (a/with {:foo 1})
                          (a/where [:> :metric 10])
                          (a/where [:always-true]
                                   (a/critical))
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

(deftest compile-io!-test
  (let [io-compiled (stream/compile-io! {:type :file
                                         :confg {:path "/tmp/foo"}})]
    (is (satisfies? IO (:component io-compiled)))))

(deftest streams-names-test
  (is (= #{:foo :bar}
         (stream/streams-names {:foo {} :bar {}})))
  (is (= #{}
         (stream/streams-names {}))))

(deftest new-config-test
  (testing "same config"
    (let [old-config {:foo {} :bar {}}
          new-config {:foo {} :bar {}}]
      (is (empty? (:to-remove (stream/new-config old-config new-config))))
      (is (empty? (:to-add (stream/new-config old-config new-config))))
      (is (empty? (:to-reload (stream/new-config old-config new-config))))))
  (testing "to-add"
    (let [old-config {:foo {} :bar {}}
          new-config {:foo {} :bar {} :baz {}}]
      (is (empty? (:to-remove (stream/new-config old-config new-config))))
      (is (= #{:baz} (:to-add (stream/new-config old-config new-config))))
      (is (empty? (:to-reload (stream/new-config old-config new-config))))))
  (testing "to-reload"
    (let [old-config {:foo {} :bar {}}
          new-config {:foo {} :bar {:foo 1} :baz {}}]
      (is (empty? (:to-remove (stream/new-config old-config new-config))))
      (is (= #{:baz} (:to-add (stream/new-config old-config new-config))))
      (is (= #{:bar} (:to-reload (stream/new-config old-config new-config))))))
    (testing "to-remove"
    (let [old-config {:foo {} :bar {}}
          new-config {:foo {}}]
      (is (= #{:bar} (:to-remove (stream/new-config old-config new-config))))
      (is (empty? (:to-add (stream/new-config old-config new-config))))
      (is (empty? (:to-reload (stream/new-config old-config new-config)))))))

(deftest async-queue-test
  (let [recorder (atom [])
        queue {:component (pool/dynamic-thread-pool-executor {})}
        stream {:name "my-stream"
                :description "foo"
                :actions (a/async-queue! :foo
                          (a/test-action recorder))}
        {:keys [entrypoint]} (stream/compile-stream! {:io {:foo queue}} stream)]
    (entrypoint {:metric 12 :time 1})
    (entrypoint {:metric 13 :time 1})
    (Thread/sleep 200)
    (is (= [{:metric 12 :time 1}
            {:metric 13 :time 1}]
           @recorder))
    (pool/shutdown (:component queue))))

(deftest stream-component-test
  (let [streams-path (.getPath (io/resource "streams"))
        io-path (.getPath (io/resource "ios"))
        streams {:foo {:actions {:action :fixed-event-window
                                 :params [100]
                                 :children []}}
                 :bar {:actions {:action :above-dt
                                 :params [[:> :metric 100] 200]
                                 :children []}}}
        new-streams {:bar {:actions {:action :above-dt
                                     :params [[:> :metric 200] 200]
                                     :children []}}
                     :baz {:actions {:action :fixed-event-window
                                     :params [200]
                                     :children []}}}
        handler (StreamHandler. [streams-path]
                                [io-path]
                                (Object.)
                                {}
                                {}
                                {}
                                {}
                                {}
                                {}
                                nil
                                nil
                                nil
                                (metric/registry-component {}))]
    (spit (str streams-path "/" "streams.edn") (pr-str streams))
    (let [{:keys [compiled-real-time-streams
                  streams-configurations]} (stream/reload handler)]
      (is (= streams-configurations streams))
      (is (= (set (keys compiled-real-time-streams)) #{:foo :bar})))
    (spit (str streams-path "/" "streams.edn") (pr-str new-streams))
    (let [{:keys [compiled-real-time-streams
                  streams-configurations]} (stream/reload handler)]
      (is (= streams-configurations new-streams))
      (is (= (set (keys compiled-real-time-streams)) #{:bar :baz})))
    (let [{:keys [compiled-real-time-streams
                  streams-configurations]} (stream/reload handler)]
      (is (= streams-configurations new-streams))
      (is (= (set (keys compiled-real-time-streams)) #{:bar :baz})))))

(deftest io-test
  (testing "Not in test mode"
    (let [custom-actions {:my-custom-action 'mirabelle.action/where*}
          recorder (atom [])
          stream {:description "foo"
                  :actions {:action :io
                            :children [{:action :test-action
                                        :params [recorder]}
                                       {:action :test-action
                                        :params [recorder]}]}}
          {:keys [entrypoint]} (stream/compile-stream!
                                {:custom-actions custom-actions}
                                stream)]
     (is (fn? entrypoint))
     (entrypoint {:metric 12})
     (is (= [{:metric 12} {:metric 12}]  @recorder))))
  (testing "In test mode"
    (let [custom-actions {:my-custom-action 'mirabelle.action/where*}
          recorder (atom [])
          stream {:description "foo"
                  :actions {:action :io
                            :children [{:action :test-action
                                        :params [recorder]}
                                       {:action :test-action
                                        :params [recorder]}]}}
          {:keys [entrypoint]} (stream/compile-stream!
                                {:custom-actions custom-actions
                                 :test-mode? true}
                                stream)]
     (is (fn? entrypoint))
     (entrypoint {:metric 12})
     (is (= []  @recorder)))))
