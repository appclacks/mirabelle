(ns mirabelle.action-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            [mirabelle.action :as a]
            [mirabelle.time :as time]))

(defn recorder
  []
  (let [state (atom [])]
    [(fn [event]
       (swap! state conj event))
     state]))

(defn test-actions
  [action state input expected]
  (reset! state [])
  (doseq [event input]
    (action event))
  (is (= expected @state)))

(deftest where*-test
  (let [[rec state] (recorder)]
    (test-actions (a/where* nil [:pos? :metric] rec)
                  state
                  [{:metric 10} {:metric -1}]
                  [{:metric 10}])
    (test-actions (a/where* nil [:pos? [:foo :metric]] rec)
                  state
                  [{:foo {:metric 10}} {:foo {:metric 11}} {:foo {:metric -7}}]
                  [{:foo {:metric 10}} {:foo {:metric 11}}])
    (test-actions (a/where* nil [:pos? :metric] rec)
                  state
                  [{:metric -1} {:metric 30} {:metric 0}]
                  [{:metric 30}])
    (test-actions (a/where* nil [:> :metric 20] rec)
                  state
                  [{:metric -1} {:metric 30} {:metric 0}]
                  [{:metric 30}])
    (test-actions (a/where* nil [:> [:foo :bar :metric] 20] rec)
                  state
                  [{:foo {:bar {:metric -1}}} {:foo {:bar {:metric 10}}} {:foo {:bar {:metric 31}}}]
                  [{:foo {:bar {:metric 31}}}])
    (test-actions (a/where* nil [:> :metric 20] rec)
                  state
                  [{:metric -1} {:metric 30} {:metric 40}]
                  [{:metric 30} {:metric 40}])
    (test-actions (a/where* nil
                            [:and
                             [:> :metric 20]
                             [:< :metric 40]]
                            rec)
                  state
                  [{:metric -1} {:metric 30} {:metric 31} {:metric 50}]
                  [{:metric 30} {:metric 31}])
    (test-actions (a/where* nil
                            [:or
                             [:< :metric 20]
                             [:> :metric 40]]
                            rec)
                  state
                  [{:metric -1} {:metric 30} {:metric 31} {:metric 50}]
                  [{:metric -1} {:metric 50}])
    (test-actions (a/where* nil
                            [:regex :host "foo.*"]
                            rec)
                  state
                  [{:host "bar" :metric 1}
                   {:host "foo" :metric 2}
                   {:host "foobar" :metric 2}
                   {:host "baz" :metric 3}]
                  [{:host "foo" :metric 2}
                   {:host "foobar" :metric 2}])
    (test-actions (a/where* nil
                            [:regex :host #"foo.*"]
                            rec)
                  state
                  [{:host "bar" :metric 1}
                   {:host "foo" :metric 2}
                   {:host "foobar" :metric 2}
                   {:host "baz" :metric 3}]
                  [{:host "foo" :metric 2}
                   {:host "foobar" :metric 2}])
    (test-actions (a/where* nil [:contains :tags "foo"] rec)
                  state
                  [{:metric 10} {:metric 10 :tags ["foo"]} {:tags []}]
                  [{:metric 10 :tags ["foo"]}])
    (test-actions (a/where* nil [:absent :tags "foo"] rec)
                  state
                  [{:metric 10} {:metric 10 :tags ["foo"]} {:tags []}]
                  [{:metric 10} {:tags []}])
    (test-actions (a/where* nil [:and
                                 [:or
                                  [:pos? :metric]
                                  [:= :metric -1]]
                                 [:and
                                  [:= :service "foo"]
                                  [:= :env "prod"]]] rec)
                  state
                  [{:metric 10 :service "foo" :env "prod"}
                   {:metric 10 :service "foo" :env "dev"}
                   {:metric 10 :service "bar" :env "prod"}
                   {:metric -2 :service "foo" :env "prod"}
                   {:metric -1 :service "foo" :env "prod"}]
                  [{:metric 10 :service "foo" :env "prod"}
                   {:metric -1 :service "foo" :env "prod"}])
    (test-actions (a/where* nil [:= [:attributes :host] "foo"] rec)
                  state
                  [{:attributes {}}
                   {:attributes {:host "foo"}}]
                  [{:attributes {:host "foo"}}])
    (test-actions (a/where* nil [:and
                                 [:and
                                  [:= :host "bar"]
                                  [:or
                                   [:pos? :metric]
                                   [:= :metric -1]]]
                                 [:= :service "foo"]] rec)
                  state
                  [{:host "bar" :metric 10 :service "foo"}
                   {:host "foo" :metric 10 :service "foo"}
                   {:host "bar" :metric -1 :service "foo"}
                   {:host "bar" :metric -2 :service "foo"}
                   {:host "bar" :metric 10 :service "bar"}]
                  [{:host "bar" :metric 10 :service "foo"}
                   {:host "bar" :metric -1 :service "foo"}])))

(deftest increment*-test
  (let [[rec state] (recorder)]
    (test-actions (a/increment* nil rec)
                  state
                  [{:metric 10} {:metric 11}]
                  [{:metric 11} {:metric 12}])))

(deftest decrement*-test
  (let [[rec state] (recorder)]
    (test-actions (a/increment* nil rec)
                  state
                  [{:metric 10} {:metric 11}]
                  [{:metric 11} {:metric 12}])))

(deftest fixed-event-window*-test
  (let [[rec state] (recorder)]
    (test-actions (a/fixed-event-window* nil {:size 3} rec)
                  state
                  [{:metric 10} {:metric 11}]
                  [])
    (test-actions (a/fixed-event-window* nil {:size 3} rec)
                  state
                  [{:metric 10} {:metric 11} {:metric 12}
                   {:metric 13} {:metric 14} {:metric 15}
                   {:metric 16}]
                  [[{:metric 10} {:metric 11} {:metric 12}]
                   [{:metric 13} {:metric 14} {:metric 15}]])))

(deftest coll-mean*-test
  (let [[rec state] (recorder)]
    (test-actions (a/coll-mean* nil rec)
                  state
                  [[{:metric 10}]]
                  [{:metric 10}])
    (test-actions (a/coll-mean* nil rec)
                  state
                  [[{:metric 10} {:metric 12}]]
                  [{:metric 11}])
    (test-actions (a/coll-mean* nil rec)
                  state
                  [[{:metric 10}]]
                  [{:metric 10}])
    (test-actions (a/coll-mean* nil rec)
                  state
                  [[{:metric 10 :time 3 :host "foo"}
                    {:metric 20 :time 1 :host "bar"}
                    {:metric 30 :time 2 :host "baz"}]]
                  [{:metric 20 :time 3 :host "foo"}])))

(deftest sdo-test
  (let [[rec state] (recorder)]
    (test-actions (a/sdo* nil rec)
                  state
                  [{:metric 10}]
                  [{:metric 10}])))

(deftest expired-test
  (let [[rec state] (recorder)]
    (test-actions (a/expired* nil rec)
                  state
                  [{:state "expired"}
                   {:state "ok"}
                   {:time 60}
                   {:time 10}
                   {:time 10 :ttl 10}
                   {:time 10 :ttl 50}]
                  [{:state "expired"}
                   {:time 10 :ttl 10}])))

(deftest not-expired-test
  (let [[rec state] (recorder)]
    (test-actions (a/not-expired* nil rec)
                  state
                  [{:state "expired"}
                   {:state "ok"}
                   {:time 1}
                   {:time 60}
                   {:time 1 :ttl 10}
                   {:time 1 :ttl 120}]
                  [{:state "ok"}
                   {:time 1}
                   {:time 60}
                   {:time 1 :ttl 120}])))

(deftest cond-dt*-test
  (let [[rec state] (recorder)]
    (test-actions (a/cond-dt* nil [:> :metric 10] 10 rec)
                  state
                  [{:time 1 :metric 12}
                   {:time 4 :metric 12}
                   {:metric 12}
                   {:time 12 :metric 12}
                   {:time 22 :metric 13}
                   {:time 23 :metric 1}
                   {:metric 1}
                   {:time 25 :metric 11}
                   {:time 36 :metric 12}]
                  [{:time 12 :metric 12}
                   {:time 22 :metric 13}
                   {:time 36 :metric 12}])))

(deftest default*-test
  (let [[rec state] (recorder)]
    (test-actions (a/default* nil :state "ok" rec)
                  state
                  [{:metric 1}
                   {:state "critical" :metric 2}
                   {:state "critical" :metric 3}
                   {:state "ok" :metric 4}
                   {:metric 5}
                   {:state "critical" :metric 6}]
                  [{:state "ok" :metric 1}
                   {:state "critical" :metric 2}
                   {:state "critical" :metric 3}
                   {:state "ok" :metric 4}
                   {:state "ok" :metric 5}
                   {:state "critical" :metric 6}])
    (test-actions (a/default* nil [:labels :state] "ok" rec)
                  state
                  [{:metric 1}
                   {:labels {:state "critical"} :metric 2}
                   {:labels {:state "critical"} :metric 3}
                   {:labels {:state "ok"} :metric 4}
                   {:labels {} :metric 5}
                   {:labels {:state "critical"} :metric 6}]
                  [{:labels {:state "ok"} :metric 1}
                   {:labels {:state "critical"} :metric 2}
                   {:labels {:state "critical"} :metric 3}
                   {:labels {:state "ok"} :metric 4}
                   {:labels {:state "ok"} :metric 5}
                   {:labels {:state "critical"} :metric 6}])))

(deftest coalesce*-test
  (let [state (atom [])
        rec (fn [event] (swap! state conj event))
        action (a/coalesce* nil {:duration 5 :fields [:host :service]} rec)]
    (doseq [event [{:host "1" :service "foo" :metric 1 :time 0 :ttl 10}
                   {:host "1" :service "bar" :metric 1 :time 5 :ttl 10}
                   {:host "2" :service "foo" :metric 1 :time 5 :ttl 10}
                   {:host "2" :service "foo" :metric 1 :time 11 :ttl 10}
                   {:host "2" :service "foo" :metric 1 :time 14 :ttl 10}
                   {:host "2" :service "foo" :metric 1 :time 12 :ttl 10}
                   {:host "3" :service "foo" :metric 1 :time 16 :ttl 10}]]
      (action event))
    (is (= [#{{:host "1", :service "foo", :metric 1, :time 0, :ttl 10}
              {:host "1", :service "bar", :metric 1, :time 5, :ttl 10}}
            #{{:host "2", :service "foo", :metric 1, :time 11, :ttl 10}
              {:host "1", :service "bar", :metric 1, :time 5, :ttl 10}}
            #{{:host "2", :service "foo", :metric 1, :time 14, :ttl 10}
              {:host "3", :service "foo", :metric 1, :time 16, :ttl 10}}]
           (map set @state))))
  (let [state (atom [])
        rec (fn [event] (swap! state conj event))
        action (a/coalesce* nil {:duration 5 :fields [:host :service]} rec)]
    (doseq [event [{:host "1" :service "foo" :metric 1 :time 0 :ttl 10}
                   {:host "1" :service "bar" :metric 1 :time 5 :ttl 10}]]
      (action event))
    (is (= [#{{:host "1", :service "foo", :metric 1, :time 0, :ttl 10}
              {:host "1", :service "bar", :metric 1, :time 5, :ttl 10}}]
           (map set @state))))
  (let [state (atom [])
        rec (fn [event] (swap! state conj event))
        action (a/coalesce* nil {:duration 5 :fields [:host :service]} rec)]
    (doseq [event [{:host "1" :service "foo" :metric 1 :time 0 :ttl 20}
                   {:host "1" :service "baz" :metric 1 :time 1 :ttl 20}
                   {:host "1" :service "bar" :metric 1 :time 12 :ttl 20}]]
      (action event))
    (is (= [#{{:host "1" :service "foo" :metric 1 :time 0 :ttl 20}
              {:host "1" :service "baz" :metric 1 :time 1 :ttl 20}
              {:host "1" :service "bar" :metric 1 :time 12 :ttl 20}}]
           (map set @state))))
  (let [state (atom [])
        rec (fn [event] (swap! state conj event))
        action (a/coalesce* nil {:duration 5 :fields [:host [:nested :service]]} rec)]
    (doseq [event [{:host "1" :nested {:service "foo"} :metric 1 :time 0 :ttl 20}
                   {:host "1" :nested {:service "baz"} :metric 1 :time 1 :ttl 20}
                   {:host "1" :nested {:service "bar"} :metric 1 :time 12 :ttl 20}]]
      (action event))
    (is (= [#{{:host "1" :nested {:service "foo"} :metric 1 :time 0 :ttl 20}
              {:host "1" :nested {:service "baz"} :metric 1 :time 1 :ttl 20}
              {:host "1" :nested {:service "bar"} :metric 1 :time 12 :ttl 20}}]
           (map set @state)))))

(deftest with*-test
  (let [[rec state] (recorder)]
    (test-actions (a/with* nil {:state "ok"} rec)
                  state
                  [{:metric 1}
                   {:state "critical" :metric 2}
                   {:state "critical" :metric 3}
                   {:state "ok" :metric 4}
                   {:metric 5}
                   {:state "critical" :metric 6}]
                  [{:state "ok" :metric 1}
                   {:state "ok" :metric 2}
                   {:state "ok" :metric 3}
                   {:state "ok" :metric 4}
                   {:state "ok" :metric 5}
                   {:state "ok" :metric 6}])
    (test-actions (a/with* nil {[:nested :state] "ok"} rec)
                  state
                  [{:metric 1}
                   {:nested {:state "critical"} :metric 2}]
                  [{:nested {:state "ok"} :metric 1}
                   {:nested {:state "ok"} :metric 2}])))

(deftest coll-max*-test
  (let [[rec state] (recorder)]
    (test-actions (a/coll-max* nil rec)
                  state
                  [[{:metric 1}
                    {:metric 10}
                    {}
                    {}
                    {:metric 5}]]
                  [{:metric 10}])))

(deftest coll-quotient*-test
  (let [[rec state] (recorder)]
    (test-actions (a/coll-quotient* nil rec)
                  state
                  [[{:metric 1}
                    {:metric 10}
                    {:metric 5}]]
                  [{:metric (/ 1 10 5)}])))

(deftest coll-sum*-test
  (let [[rec state] (recorder)]
    (test-actions (a/coll-sum* nil rec)
                  state
                  [[{:metric 1}
                    {:metric 10 :time 0 :tags ["a"]}
                    {}
                    {}
                    {:metric 5}]]
                  [{:metric 16  :time 0 :tags ["a"]}])))

(deftest coll-min*-test
  (let [[rec state] (recorder)]
    (test-actions (a/coll-min* nil rec)
                  state
                  [[{:metric 1}
                    {:metric 10}
                    {}
                    {}
                    {:metric 5}]]
                  [{:metric 1}])))

(deftest coll-rate*-test
  (let [[rec state] (recorder)]
    (test-actions (a/coll-rate* nil rec)
                  state
                  [[{:metric 1 :time 1}]]
                  [{:metric 1 :time 1}])
    (test-actions (a/coll-rate* nil rec)
                  state
                  [[{:metric 1 :time 1}
                    {:metric 10 :time 2}
                    {:metric 4 :time 3}
                    {:metric 10 :time 1}
                    {:metric 5 :time 4}]]
                  [{:time 4 :metric (/ 30 3)}])
    (test-actions (a/coll-rate* nil rec)
                  state
                  [[{:metric 1 :time 0}
                    {:metric 1 :time 2}
                    {:metric 1 :time 3}
                    {:metric 1 :time 1}
                    {:metric 1 :time 10}]]
                  [{:time 10 :metric (/ 5 10)}])
    (test-actions (a/coll-rate* nil rec)
                  state
                  [[{:metric 1 :time 1}
                    {:metric 2 :time 2}
                    {:metric 1 :time 3}]]
                  [{:time 3 :metric 2}])))

(deftest sflatten*-test
  (let [[rec state] (recorder)]
    (test-actions (a/sflatten* nil rec)
                  state
                  [[{:metric 1 :time 1}
                    {:metric 10 :time 2}
                    {:metric 4 :time 3}
                    {:metric 10 :time 1}
                    {:metric 5 :time 4}]]
                  [{:metric 1 :time 1}
                   {:metric 10 :time 2}
                   {:metric 4 :time 3}
                   {:metric 10 :time 1}
                   {:metric 5 :time 4}])))

(deftest tag*-test
  (let [[rec state] (recorder)]
    (test-actions (a/tag* nil "foo" rec)
                  state
                  [{:metric 1 :time 1}
                   {:metric 2 :tags ["foo"]}
                   {:metric 10 :time 1 :tags ["bar"]}
                   {:metric 5 :time 4 :tags ["foo"]}]
                  [{:metric 1 :time 1 :tags ["foo"]}
                   {:metric 2 :tags ["foo"]}
                   {:metric 10 :time 1 :tags ["foo" "bar"]}
                   {:metric 5 :time 4 :tags ["foo"]}]))
  (let [[rec state] (recorder)]
    (test-actions (a/tag* nil ["foo" "bar"] rec)
                  state
                  [{:metric 1 :time 1}
                   {:metric 2 :tags ["foo"]}
                   {:metric 10 :time 1 :tags ["bar"]}
                   {:metric 5 :time 4 :tags ["foo"]}]
                  [{:metric 1 :time 1 :tags ["foo" "bar"]}
                   {:metric 2 :tags ["foo" "bar"]}
                   {:metric 10 :time 1 :tags ["foo" "bar"]}
                   {:metric 5 :time 4 :tags ["foo" "bar"]}])))

(deftest untag*-test
  (let [[rec state] (recorder)]
    (test-actions (a/untag* nil "foo" rec)
                  state
                  [{:metric 1 :time 1}
                   {:metric 2 :tags ["foo"]}
                   {:metric 10 :time 1 :tags ["bar"]}
                   {:metric 5 :time 4 :tags ["foo" "bar"]}]
                  [{:metric 1 :time 1 :tags []}
                   {:metric 2 :tags []}
                   {:metric 10 :time 1 :tags ["bar"]}
                   {:metric 5 :time 4 :tags ["bar"]}])
    (test-actions (a/untag* nil ["foo" "bar"] rec)
                  state
                  [{:metric 1 :time 1}
                   {:metric 2 :tags ["foo"]}
                   {:metric 10 :time 1 :tags ["bar"]}
                   {:metric 5 :time 4 :tags ["foo" "bar"]}]
                  [{:metric 1 :time 1 :tags []}
                   {:metric 2 :tags []}
                   {:metric 10 :time 1 :tags []}
                   {:metric 5 :time 4 :tags []}])))

(deftest dtt*-test
  (let [[rec state] (recorder)]
    (test-actions (a/ddt* nil false rec)
                  state
                  [{:metric 1 :time 1}
                   {:metric 10 :time 4}
                   {:metric 11 :time 7}
                   {:metric 8 :time 10}]
                  [{:metric (/ 9 3) :time 4}
                   {:metric (/ 1 3) :time 7}
                   {:metric -1 :time 10}])
    (test-actions (a/ddt* nil true rec)
                  state
                  [{:metric 1 :time 1}
                   {:metric 10 :time 4}
                   {:metric 11 :time 7}
                   {:metric 0 :time 10}
                   {:time 12}
                   {:metric 4 :time 12}]
                  [{:metric (/ 9 3) :time 4}
                   {:metric (/ 1 3) :time 7}
                   {:metric 2 :time 12}])))

(deftest scale*-test
  (let [[rec state] (recorder)]
    (test-actions (a/scale* nil 10 rec)
                  state
                  [{:metric 1 :time 1}
                   {:metric 10 :time 4}
                   ]
                  [{:metric 10 :time 1}
                   {:metric 100 :time 4}])))

(deftest split*-test
  (let [[rec state] (recorder)]
    (test-actions (a/split* nil
                            [[:= :state "critical"]]
                            rec)
                  state
                  [{:metric 1 :time 1 :state "ok"}
                   {:metric 1 :time 1 :state "warning"}
                   {:metric 1 :time 1 :state "critical"}]
                  [{:metric 1 :time 1 :state "critical"}]))
  (let [[rec state] (recorder)
        [rec2 state2] (recorder)]
    (test-actions (a/split* nil
                            [[:= :state "critical"]
                             [:= :state "ok"]]
                            rec
                            rec2)
                  state
                  [{:metric 1 :time 1 :state "ok"}
                   {:metric 1 :time 1 :state "warning"}
                   {:metric 1 :time 1}
                   {:metric 10 :time 4 :state "critical"}
                   {:metric 1 :time 1 :state "foo"}
                   {:metric 100 :time 1 :state "ok"}]
                  [{:metric 10 :time 4 :state "critical"}])
    (is (= [{:metric 1 :time 1 :state "ok"}
            {:metric 100 :time 1 :state "ok"}]
           @state2))))

(deftest throttle*-test
  (let [[rec state] (recorder)]
    (test-actions (a/throttle* nil
                               {:count 1 :duration 5}
                               rec)
                  state
                  [{:metric 1 :time 0 :state "ok"}
                   {:metric 1 :time 1 :state "ok"}
                   {:metric 1 :time 5 :state "ok"}
                   {:metric 1 :time 7 :state "ok"}
                   {:metric 1 :time 3 :state "ok"}
                   {:metric 1 :time 12 :state "ok"}
                   {:metric 1 :time 14 :state "ok"}
                   {:metric 1 :time 16 :state "ok"}
                   {:metric 1 :time 18 :state "ok"}]
                  [{:metric 1 :time 0 :state "ok"}
                   {:metric 1 :time 5 :state "ok"}
                   {:metric 1 :time 12 :state "ok"}
                   {:metric 1 :time 18 :state "ok"}]))
  (let [[rec state] (recorder)]
    (test-actions (a/throttle* nil
                               {:count 2 :duration 5}
                               rec)
                  state
                  [{:metric 1 :time 0 :state "ok"}
                   {:metric 1 :time 1 :state "ok"}
                   {:metric 1 :time 5 :state "ok"}
                   {:metric 1 :time 7 :state "ok"}
                   {:metric 1 :time 3 :state "ok"}
                   {:metric 1 :time 12 :state "ok"}
                   {:metric 1 :time 14 :state "ok"}
                   {:metric 1 :time 16 :state "ok"}
                   {:metric 1 :time 18 :state "ok"}]
                  [{:metric 1 :time 0 :state "ok"}
                   {:metric 1 :time 1 :state "ok"}
                   {:metric 1 :time 5 :state "ok"}
                   {:metric 1 :time 7 :state "ok"}
                   {:metric 1 :time 12 :state "ok"}
                   {:metric 1 :time 14 :state "ok"}
                   {:metric 1 :time 18 :state "ok"}])))

(deftest fixed-time-window*-test
  (testing "no delay"
    (let [[rec state] (recorder)]
      (test-actions (a/aggregation* nil
                                    {:duration 5 :aggr-fn :fixed-time-window}
                                    rec)
                    state
                    [{:metric 1 :time 0 :state "ok"}
                     {:metric 1 :time 1 :state "ok"}
                     {:metric 1 :time 3 :state "ok"}
                     {:metric 1 :time 5 :state "ok"}
                     {:metric 1 :time 7 :state "ok"}
                     {:metric 1 :time 9 :state "ok"}
                     {:metric 1 :time 10 :state "ok"}
                     {:metric 1 :time 29 :state "ok"}
                     {:metric 1 :time 31 :state "ok"}]
                    [[{:metric 1 :time 0 :state "ok"}
                      {:metric 1 :time 1 :state "ok"}
                      {:metric 1 :time 3 :state "ok"}]
                     [{:metric 1 :time 5 :state "ok"}
                      {:metric 1 :time 7 :state "ok"}
                      {:metric 1 :time 9 :state "ok"}]
                     [{:metric 1 :time 10 :state "ok"}]
                     [{:metric 1 :time 29 :state "ok"}]])))
  (testing "a lot of delay"
    (let [[rec state] (recorder)]
      (test-actions (a/aggregation* nil
                                    {:duration 5
                                     :aggr-fn :fixed-time-window
                                     :delay 30}
                                    rec)
                    state
                    [{:metric -10 :time -10}
                     {:metric 1 :time 0}
                     {:metric 20 :time 3}
                     {:metric -9 :time -9}
                     {:metric 12 :time 20}
                     {:metric 24 :time 2}
                     {:metric 2 :time 34}
                     {:metric 1 :time 36}
                     ]
                    [[{:metric -10 :time -10}
                      {:metric -9 :time -9}]
                     [{:metric 1 :time 0}
                      {:metric 20 :time 3}
                      {:metric 24 :time 2}]]))))

(deftest moving-event-window*-test
  (let [[rec state] (recorder)]
    (test-actions (a/moving-event-window* nil
                                          {:size 5}
                                          rec)
                  state
                  [{:metric 1 :time 0 :state "ok"}
                   {:metric 1 :time 1 :state "ok"}
                   {:metric 1 :time 3 :state "ok"}
                   {:metric 1 :time 9 :state "ok"}
                   {:metric 1 :time 10 :state "ok"}
                   {:metric 1 :time 29 :state "ok"}]
                  [[{:metric 1 :time 0 :state "ok"}]
                   [{:metric 1 :time 0 :state "ok"}
                    {:metric 1 :time 1 :state "ok"}]
                   [{:metric 1 :time 0 :state "ok"}
                    {:metric 1 :time 1 :state "ok"}
                    {:metric 1 :time 3 :state "ok"}]
                   [{:metric 1 :time 0 :state "ok"}
                    {:metric 1 :time 1 :state "ok"}
                    {:metric 1 :time 3 :state "ok"}
                    {:metric 1 :time 9 :state "ok"}]
                   [{:metric 1 :time 0 :state "ok"}
                    {:metric 1 :time 1 :state "ok"}
                    {:metric 1 :time 3 :state "ok"}
                    {:metric 1 :time 9 :state "ok"}
                    {:metric 1 :time 10 :state "ok"}]
                   [{:metric 1 :time 1 :state "ok"}
                    {:metric 1 :time 3 :state "ok"}
                    {:metric 1 :time 9 :state "ok"}
                    {:metric 1 :time 10 :state "ok"}
                    {:metric 1 :time 29 :state "ok"}]])))

(deftest ewma-timeless*-test
  (let [[rec state] (recorder)]
    (test-actions (a/ewma-timeless* nil
                                    0
                                    rec)
                  state
                  [{:metric 1 :time 0 :state "ok"}
                   {:metric 3 :time 1 :state "ok"}
                   {:metric 5 :time 2 :state "ok"}]
                  [{:metric 0 :time 0 :state "ok"}
                   {:metric 0 :time 1 :state "ok"}
                   {:metric 0 :time 2 :state "ok"}]))
  (let [[rec state] (recorder)]
    (test-actions (a/ewma-timeless* nil
                                    1
                                    rec)
                  state
                  [{:metric 1 :time 0 :state "ok"}
                   {:metric 3 :time 1 :state "ok"}
                   {:metric 5 :time 2 :state "ok"}]
                  [{:metric 1 :time 0 :state "ok"}
                   {:metric 3 :time 1 :state "ok"}
                   {:metric 5 :time 2 :state "ok"}]))
  (let [[rec state] (recorder)]
    (test-actions (a/ewma-timeless* nil
                                    0.5
                                    rec)
                  state
                  [{:metric 1 :time 0 :state "ok"}
                   {:metric 1 :time 1 :state "ok"}
                   {:metric 1 :time 2 :state "ok"}]
                  [{:metric 0.5 :time 0 :state "ok"}
                   {:metric 0.75 :time 1 :state "ok"}
                   {:metric 0.875 :time 2 :state "ok"}])))

(deftest over*-test
  (let [[rec state] (recorder)]
    (test-actions (a/over* nil 10 rec)
                  state
                  [{:metric 10}
                   {:metric 1}
                   {:metric 4}
                   {:metric 12}
                   {:metric 11}
                   {:metric 1}
                   {:metric 9}
                   {:metric 14}]
                  [{:metric 12}
                   {:metric 11}
                   {:metric 14}])))

(deftest under*-test
  (let [[rec state] (recorder)]
    (test-actions (a/under* nil 10 rec)
                  state
                  [{:metric 10}
                   {:metric 1}
                   {:metric 4}
                   {:metric 12}
                   {:metric 11}
                   {:metric 1}
                   {:metric 9}
                   {:metric 14}]
                  [{:metric 1}
                   {:metric 4}
                   {:metric 1}
                   {:metric 9}])))

(deftest changed*-test
  (let [[rec state] (recorder)]
    (test-actions (a/changed* nil {:field :state :init "ok"} rec)
                  state
                  [{:metric 1 :state "ok"}
                   {:metric 2 :state "ok"}
                   {:metric 3 :state "critical"}
                   {:metric 4 :state "critical"}
                   {:metric 4 :state "critical"}
                   {:metric 5 :state "ok"}
                   {:metric 6 :state "critical"}]
                  [{:metric 3 :state "critical"}
                   {:metric 5 :state "ok"}
                   {:metric 6 :state "critical"}]))
  (let [[rec state] (recorder)]
    (test-actions (a/changed* nil {:field [:nested :state] :init "ok"} rec)
                  state
                  [{:metric 1 :nested {:state "ok"}}
                   {:metric 2 :nested {:state "ok"}}
                   {:metric 3 :nested {:state "critical"}}
                   {:metric 4 :nested {:state "critical"}}
                   {:metric 4 :nested {:state "critical"}}
                   {:metric 5 :nested {:state "ok"}}
                   {:metric 6 :nested {:state "critical"}}]
                  [{:metric 3 :nested {:state "critical"}}
                   {:metric 5 :nested {:state "ok"}}
                   {:metric 6 :nested {:state "critical"}}])))

(deftest project*-test
  (let [[rec state] (recorder)]
    (test-actions (a/project* nil
                              [[:= :service "foo"]
                               [:= :service "bar"]]
                              rec)
                  state
                  [{:time 1 :service "foo" :ttl 10}
                   {:time 2 :service "foo" :ttl 10}
                   {:time 3 :service "bar" :ttl 10}
                   {:time 2 :service "foo" :ttl 10}
                   {:time 2 :service "bar" :ttl 10}
                   {:time 4 :service "trololo" :ttl 10}
                   ;; make everything expire
                   {:time 30 :service "trololo" :ttl 10}
                   {:time 31 :service "foo" :ttl 10}
                   {:time 20 :service "bar" :ttl 10}
                   {:time 29 :service "bar" :ttl 10}
                   {:metric 10 :service "bar" :ttl 10}
                   ]
                  [[{:time 1 :service "foo" :ttl 10}]
                   [{:time 2 :service "foo" :ttl 10}]
                   [{:time 2 :service "foo" :ttl 10}
                    {:time 3 :service "bar" :ttl 10}]
                   [{:time 2 :service "foo" :ttl 10}
                    {:time 3 :service "bar" :ttl 10}]
                   [{:time 2 :service "foo" :ttl 10}
                    {:time 3 :service "bar" :ttl 10}]
                   [{:time 2 :service "foo" :ttl 10}
                    {:time 3 :service "bar" :ttl 10}]
                   [{:time 31 :service "foo" :ttl 10}]
                   [{:time 31 :service "foo" :ttl 10}]
                   [{:time 31 :service "foo" :ttl 10}
                    {:time 29 :service "bar" :ttl 10}]
                   [{:time 31 :service "foo" :ttl 10}
                    {:time 29 :service "bar" :ttl 10}]])))

(deftest count*-test
  (let [[rec state] (recorder)]
    (test-actions (a/coll-count* nil
                                 rec)
                  state
                  [[]
                   [{}]
                   [{} {}]
                   [{} {} {}]]
                  [{:metric 0}
                   {:metric 1}
                   {:metric 2}
                   {:metric 3}]))
  (let [[rec state] (recorder)]
    (test-actions (a/coll-count* nil
                                 rec)
                  state
                  [[]
                   [{}]
                   [{} {:time 2}]
                   [{} {:time 1} {}]]
                  [{:metric 0}
                   {:metric 1}
                   {:metric 2 :time 2}
                   {:metric 3 :time 1}])))

(deftest sdissoc*-test
  (let [[rec state] (recorder)]
    (test-actions (a/sdissoc* nil
                              [:foo]
                              rec)
                  state
                  [{:foo 1
                    :metric 1}
                   {:host "bar"
                    :metric 2}]
                  [{:metric 1}
                   {:host "bar"
                    :metric 2}])
    (test-actions (a/sdissoc* nil
                              [[:attributes :x-client]]
                              rec)
                  state
                  [{:attributes {:x-client 1}
                    :metric 1}]
                  [{:metric 1}])
    (test-actions (a/sdissoc* nil
                              [:foo :host]
                              rec)
                  state
                  [{:foo 1
                    :metric 1}
                   {:host "bar"
                    :metric 2}
                   {:host "bar"
                    :foo 1
                    :metric 3}]
                  [{:metric 1}
                   {:metric 2}
                   {:metric 3}])
    (test-actions (a/sdissoc* nil
                              [:foo [:nested :key]]
                              rec)
                  state
                  [{:foo 1
                    :metric 1}
                   {:nested {:key 1}
                    :metric 2}
                   {:nested {:key 2}
                    :foo 1
                    :metric 3}]
                  [{:metric 1}
                   {:metric 2}
                   {:metric 3}])))

(deftest coll-percentiles*-test
  (let [[rec state] (recorder)]
    (test-actions (a/coll-percentiles* nil
                                       [0 0.5 1]
                                       rec)
                  state
                  [[{:metric 3} {:metric 1} {:metric 2}]]
                  [{:metric 1 :quantile "0"}
                   {:metric 2 :quantile "0.5"}
                   {:metric 3 :quantile "1"}])))

(deftest tagged-all*-test
  (let [[rec state] (recorder)]
    (test-actions (a/tagged-all* nil
                                 "foo"
                                 rec)
                  state
                  [{:metric 3} {:metric 1 :tags ["a"]} {:metric 2 :tags ["foo"]}]
                  [{:metric 2 :tags ["foo"]}])
    (test-actions (a/tagged-all* nil
                                 ["foo" "bar"]
                                 rec)
                  state
                  [{:metric 3}
                   {:metric 1 :tags ["a"]}
                   {:metric 2 :tags ["foo"]}
                   {:metric 3 :tags ["foo" "bar"]}]
                  [{:metric 3 :tags ["foo" "bar"]}])))

(deftest from-json*-test
  (let [[rec state] (recorder)]
    (test-actions (a/from-json* nil
                                [:foo]
                                rec)
                  state
                  [{:foo "{\"bar\": \"baz\"}"}]
                  [{:foo {:bar "baz"}}])
    (test-actions (a/from-json* nil
                                [:foo :bar]
                                rec)
                  state
                  [{:foo {:bar "{\"bar\": \"baz2\"}"}
                    :host "h"}]
                  [{:foo {:bar {:bar "baz2"}}
                    :host "h"}])
    (test-actions (a/from-json* nil
                                [:foo]
                                rec)
                  state
                  [{:host "aa"}]
                  [{:host "aa" :foo nil}])))

(deftest exception-stream*-test
  (let [[rec state] (recorder)
        stream (a/exception-stream* nil
                                    (fn [_]
                                      (throw (ex-info "boom" {})))
                                    rec)]
    (stream {:foo 1})
    (is (= 1 (count @state)))
    (is (= "error" (:state (first @state))))
    (is (= "mirabelle-exception" (:service (first @state))))
    (is (instance? Exception (:exception (first @state))))))

(deftest streams-test
  (is (= {:foo {:actions {:action :sdo
                          :description {:message "Forward events to children"}
                          :children [{:action :increment
                                      :description {:message "Increment the :metric field"}
                                      :children nil}]}}}
         (a/streams
          (a/stream
           {:name :foo}
           (a/increment)))))
  (is (= {:foo {:actions {:action :sdo
                          :description {:message "Forward events to children"}
                          :children [{:action :increment
                                      :description {:message "Increment the :metric field"}
                                      :children nil}]}}
          :bar {:actions {:action :sdo
                          :description {:message "Forward events to children"}
                          :children [{:action :decrement
                                      :description {:message "Decrement the :metric field"}
                                      :children nil}
                                     {:action :increment
                                      :description {:message "Increment the :metric field"}
                                      :children nil}]}}}
         (a/streams
          (a/stream
           {:name :foo}
           (a/increment))
          (a/stream
           {:name :bar}
           (a/decrement)
           (a/increment))))))

(deftest custom-test
  (is (= {:action :foo
          :params []
          :description {:message "Use the custom action :foo", :params ""}
          :children [{:action :decrement
                      :description {:message "Decrement the :metric field"}
                      :children nil}]}
         (a/custom :foo nil
                   (a/decrement))))
  (is (= {:action :foo
          :params []
          :description {:message "Use the custom action :foo", :params "[]"}
          :children [{:action :decrement
                      :description {:message "Decrement the :metric field"}
                      :children nil}]}
         (a/custom :foo []
                   (a/decrement))))
  (is (= {:action :foo
          :params [:a 1 "a"]
          :description {:message "Use the custom action :foo", :params "[:a 1 \"a\"]"}
          :children [{:action :decrement
                      :description {:message "Decrement the :metric field"}
                      :children nil}]}
         (a/custom :foo [:a 1 "a"]
                   (a/decrement)))))

(deftest to-base64*-test
  (let [[rec state] (recorder)]
    (test-actions (a/to-base64* nil
                                :host
                                rec)
                  state
                  [{:host "aa" :service "bb"}
                   {:host "bb" :service "aa" :state "critical"}]
                  [{:host "YWE=" :service "bb"}
                   {:host "YmI=" :service "aa" :state "critical"}])
    (test-actions (a/to-base64* nil
                                [:foo :bar]
                                rec)
                  state
                  [{:foo {:bar "aa"} :service "bb"}
                   {:foo {:bar "bb"} :service "aa" :state "critical"}]
                  [{:foo {:bar "YWE="} :service "bb"}
                   {:foo {:bar "YmI="} :service "aa" :state "critical"}])))

(deftest from-base64*-test
  (let [[rec state] (recorder)]
    (test-actions (a/from-base64* nil
                                  :host
                                  rec)
                  state
                  [{:host "YWE=" :service "YmI="}
                   {:host "YmI=" :service "YWE=" :state "critical"}]
                  [{:host "aa" :service "YmI="}
                   {:host "bb" :service "YWE=" :state "critical"}])
    (test-actions (a/from-base64* nil
                                  [:host :bar]
                                  rec)
                  state
                  [{:host {:bar "YWE="} :service "YmI="}
                   {:host {:bar "YmI="} :service "YWE=" :state "critical"}]
                  [{:host {:bar "aa"} :service "YmI="}
                   {:host {:bar "bb"} :service "YWE=" :state "critical"}])))

(deftest sformat*-test
  (let [[rec state] (recorder)]
    (test-actions (a/sformat* nil
                              "%s-cc-%s"
                              :format-test
                              [:host :service]
                              rec)
                  state
                  [{:host "aa" :service "bb"}
                   {:host "bb" :service "aa" :state "critical"}]
                  [{:host "aa" :service "bb" :format-test "aa-cc-bb"}
                   {:host "bb"
                    :service "aa"
                    :state "critical"
                    :format-test "bb-cc-aa"}]))
  (let [[rec state] (recorder)]
    (test-actions (a/sformat* nil
                              "%s-cc"
                              :service
                              [:host]
                              rec)
                  state
                  [{:host "aa" :service "bb"}]
                  [{:host "aa" :service "aa-cc"}]))
  (let [[rec state] (recorder)]
    (test-actions (a/sformat* nil
                              "%s-cc-%s"
                              :service
                              [[:host :h] :foo]
                              rec)
                  state
                  [{:host {:h "aa"} :service "bb" :foo "bar"}]
                  [{:host {:h "aa"} :service "aa-cc-bar" :foo "bar"}]))
  (let [[rec state] (recorder)]
    (test-actions (a/sformat* nil
                              "%s-cc-%s"
                              [:a :service]
                              [[:host :h] :foo]
                              rec)
                  state
                  [{:host {:h "aa"} :service "bb" :foo "bar"}]
                  [{:host {:h "aa"} :service "bb" :a {:service "aa-cc-bar"} :foo "bar"}])))

(deftest coll-top-test
  (let [[rec state] (recorder)]
    (test-actions (a/coll-top* nil
                               2
                               rec)
                  state
                  [[{:metric 10} {:metric 4} {:metric 100} {:metric 2}]
                   [{:metric 10}]
                   [{:metric -10} {:metric 1} {:metric 2} {:metric 0}]]
                  [[{:metric 100} {:metric 10}]
                   [{:metric 10}]
                   [{:metric 2} {:metric 1}]])))

(deftest coll-bottom-test
  (let [[rec state] (recorder)]
    (test-actions (a/coll-bottom* nil
                                  2
                                  rec)
                  state
                  [[{:metric 10} {:metric 4} {:metric 100} {:metric 2}]
                   [{:metric 10}]
                   [{:metric -10} {:metric 1} {:metric 2} {:metric 0}]]
                  [[{:metric 2} {:metric 4}]
                   [{:metric 10}]
                   [{:metric -10} {:metric 0}]])))

(deftest stable*-test
  (let [[rec state] (recorder)]
    (test-actions (a/stable* nil
                             10
                             :state
                             rec)
                  state
                  [{:state "critical" :time 1}
                   {:state "critical" :time 9}
                   {:state "critical" :time 12}
                   {:state "critical" :time 13}
                   {:state "ok" :time 1}
                   {:state "ok" :time 14}
                   {:state "ok" :time 1}
                   {:state "ok" :time 15}
                   {:state "ok" :time 25}
                   {:state "critical" :time 28}
                   {:state "ok" :time 29}
                   {:state "ok" :time 38}
                   {:state "ok" :time 40}
                   {:state "critical" :time 40}
                   {:state "ok" :time 41}
                   {:state "ok" :time 52}]
                  [{:state "critical" :time 1}
                   {:state "critical" :time 9}
                   {:state "critical" :time 12}
                   {:state "critical" :time 13}
                   ;;
                   {:state "ok" :time 14}
                   {:state "ok" :time 15}
                   {:state "ok" :time 25}
                   ;;
                   {:state "ok" :time 29}
                   {:state "ok" :time 38}
                   {:state "ok" :time 40}
                   {:state "ok" :time 41}
                   {:state "ok" :time 52}])
    (test-actions (a/stable* nil
                             10
                             [:nested :state]
                             rec)
                  state
                  [{:nested {:state "critical"} :time 1}
                   {:nested {:state "critical"} :time 9}
                   {:nested {:state "critical"} :time 12}
                   {:nested {:state "critical"} :time 13}
                   ]
                  [{:nested {:state "critical"} :time 1}
                   {:nested {:state "critical"} :time 9}
                   {:nested {:state "critical"} :time 12}
                   {:nested {:state "critical"} :time 13}])))

(deftest rename-keys*-test
  (let [[rec state] (recorder)]
    (test-actions (a/rename-keys* nil
                                  {:host :service
                                   :environment :env}
                                  rec)
                  state
                  [{:host "foo" :service "bar" :environment "prod"}
                   {:host "foo" :service "bar"}
                   {:service "bar"}
                   {}]
                  [{:service "foo" :env "prod"}
                   {:service "foo"}
                   {:service "bar"}
                   {}])
    (test-actions (a/rename-keys* nil
                                  {[:attributes :foo] :service
                                   [:attributes :bar] [:attributes :baz]}
                                  rec)
                  state
                  [{:host "foo" :service "bar" :environment "prod" :attributes {}}
                   {:attributes {:foo "1"
                                 :bar "2"}}]
                  [{:host "foo" :service "bar" :environment "prod" :attributes {}}
                   {:service "1"
                    :attributes {:baz "2"}}])

    (test-actions (a/rename-keys* nil
                                  {:host [:attributes :host]
                                   [:attributes :foo] :service
                                   [:attributes :bar] [:attributes :baz]}
                                  rec)
                  state
                  [{:host "foo" :service "bar" :environment "prod" :attributes {}}
                   {:attributes {:foo "1"
                                 :bar "2"}}]
                  [{:service "bar"
                    :environment "prod"
                    :attributes {:host "foo"}}
                   {:service "1"
                    :attributes {:baz "2"}}])))

(deftest keep-keys*-test
  (let [[rec state] (recorder)]
    (test-actions (a/keep-keys* nil
                                [:host :service :metric]
                                rec)
                  state
                  [{:host "foo" :service "bar" :environment "prod" :metric 10}
                   {:host "foo" :service "baz" :time 4}
                   {:service "bar"}
                   {}]
                  [{:host "foo" :service "bar" :metric 10}
                   {:host "foo" :service "baz"}
                   {:service "bar"}
                   {}])
    (test-actions (a/keep-keys* nil
                                [[:foo :bar :baz] :a]
                                rec)
                  state
                  [{:a 1 :host "foo" :foo {:bar {:baz "1"
                                                 :toto "2"}
                                           :invalid true}}]
                  [{:a 1 :foo {:bar {:baz "1"}}}])))

(deftest include-test
  (let [include-path (.getPath (io/resource "include/action"))]
    (is (= {:action :where
            :description {:message "Filter events based on the provided condition",
                          :params "[:= :service \"toto\"]"},
            :params [[:= :service "toto"]]
            :children
            [{:action :with,
              :description {:message "Set the field :state to critical"}
              :children nil,
              :params [{:state "critical"}]}]}
           (a/include include-path {:variables {:service "toto"}})))
    (is (= {:action :where
            :params [[:= :service "toto"]]
            :description {:message "Filter events based on the provided condition",
                          :params "[:= :service \"toto\"]"}
            :children
            [{:action :with,
              :children nil,
              :description {:message "Set the field :state to info"}
              :params [{:state "info"}]}]}
           (a/include include-path {:variables {:service "toto"}
                                    :profile :prod})))))

(deftest aggregation*-test
  (testing "no delay"
    (let [[rec state] (recorder)]
      (test-actions (a/aggregation* nil
                                    {:duration 10
                                     :aggr-fn :+
                                     :delay 0}
                                    rec)
                    state
                    [{:time 0 :metric 10}
                     {:time 7 :metric 1}
                     {:time 11 :metric 3}
                     {:time 14 :metric 8}
                     {:time 19 :metric 1}
                     {:time 20 :metric 2}
                     {:time 23 :metric 4}
                     {:time 60 :metric 1}
                     {:time 64 :metric 4}
                     {:time 70 :metric 3}]
                    [{:time 7 :metric 11}
                     {:time 19 :metric 12}
                     {:time 23 :metric 6}
                     {:time 64 :metric 5}])))
  (testing "delay"
    (let [[rec state] (recorder)]
      (test-actions (a/aggregation* nil
                                    {:duration 10
                                     :aggr-fn :+
                                     :delay 5}
                                    rec)
                    state
                    [{:time 0 :metric 10}
                     {:time 7 :metric 1}
                     {:time 11 :metric 3}
                     ;; delayed events
                     {:time 8 :metric 2}
                     {:time 9 :metric 2}
                     ;; too old
                     {:time 1 :metric 2}
                     {:time 14 :metric 8}
                     {:time 19 :metric 1}
                     {:time 20 :metric 2}
                     {:time 23 :metric 4}
                     {:time 25 :metric 4}]
                    [{:time 9 :metric 15}
                     {:time 19 :metric 12}])))
  (testing "delay negative events"
    (let [[rec state] (recorder)]
      (test-actions (a/aggregation* nil
                                    {:duration 10
                                     :aggr-fn :+
                                     :delay 5}
                                    rec)
                    state
                    [{:time 0 :metric 10}
                     ;; delayed events
                     {:time -3 :metric 10}
                     {:time -2 :metric 1}
                     ;;
                     {:time 3 :metric 1}
                     {:time 11 :metric 3}
                     {:time 8 :metric 2}
                     {:time 9 :metric 2}
                     ;; too old
                     {:time 1 :metric 2}
                     {:time 14 :metric 8}
                     {:time 19 :metric 1}
                     {:time 20 :metric 2}
                     {:time 23 :metric 4}
                     {:time 25 :metric 4}]
                    [{:time -2 :metric 11}
                     {:time 9 :metric 15}
                     {:time 19 :metric 12}]))))

(deftest coll-where*-test
  (let [[rec state] (recorder)]
    (test-actions (a/coll-where* nil [:pos? :metric] rec)
                  state
                  [[{:metric 10} {:metric 10}]]
                  [[{:metric 10} {:metric 10}]])
    (test-actions (a/coll-where* nil [:pos? :metric] rec)
                  state
                  [[{:metric -1} {:metric 30} {:metric 0}]]
                  [[{:metric 30}]])
    (test-actions (a/coll-where* nil [:pos? [:nested :metric]] rec)
                  state
                  [[{:nested {:metric -1}} {:nested {:metric 30}} {:nested {:metric 0}}]]
                  [[{:nested {:metric 30}}]])
    (test-actions (a/coll-where* nil [:> :metric 20] rec)
                  state
                  [[{:metric -1} {:metric 30} {:metric 0}]]
                  [[{:metric 30}]])
    (test-actions (a/coll-where* nil
                                 [:and
                                  [:> :metric 20]
                                  [:< :metric 40]]
                                 rec)
                  state
                  [[{:metric -1} {:metric 30} {:metric 31} {:metric 50}]]
                  [[{:metric 30} {:metric 31}]])))

(deftest coll-sort*-test
  (let [[rec state] (recorder)]
    (test-actions (a/coll-sort* nil :time rec)
                  state
                  [[{:time 11} {:time 10} {:time 3} {:time 14}]]
                  [[{:time 3} {:time 10} {:time 11} {:time 14}]])))

(deftest moving-time-window*
  (let [[rec state] (recorder)]
    (test-actions (a/moving-time-window* nil {:duration 5} rec)
                  state
                  [{:time 3}
                   {:time 5}
                   {:time 7}
                   {:time 10}
                   {:time 11}
                   {:time 13}]
                  [[{:time 3}]
                   [{:time 3} {:time 5}]
                   [{:time 3} {:time 5} {:time 7}]
                   [{:time 7} {:time 10}]
                   [{:time 7} {:time 10} {:time 11}]
                   [{:time 10} {:time 11} {:time 13}]])
    (test-actions (a/moving-time-window* nil {:duration 3} rec)
                  state
                  [{:time 1}
                   {:time 2}
                   {:time 3}
                   {:time 4}
                   {:time 5}
                   {:time 6}]
                  [[{:time 1}]
                   [{:time 1} {:time 2}]
                   [{:time 1} {:time 2} {:time 3}]
                   [{:time 2} {:time 3} {:time 4}]
                   [{:time 3} {:time 4} {:time 5}]
                   [{:time 4} {:time 5} {:time 6}]])))

(deftest ssort*-test
  (let [[rec state] (recorder)]
    (test-actions (a/aggregation* nil {:duration 5 :field :time :aggr-fn :ssort :delay 10} rec)
                  state
                  [{:time 0}
                   {:time 3}
                   {:time 2}
                   {:time 4}
                   {:time 1}
                   {:time 10}
                   {:time 14}
                   {:time 12}
                   {:time 19}
                   {:time 49}
                   {:time 47}
                   {:time 51}
                   {:time 49}
                   {:time 100}]
                  [{:time 0}
                   {:time 1}
                   {:time 2}
                   {:time 3}
                   {:time 4}
                   {:time 10}
                   {:time 12}
                   {:time 14}
                   {:time 19}
                   {:time 47}
                   {:time 49}
                   {:time 49}
                   {:time 51}]))
  (let [[rec state] (recorder)]
    (test-actions (a/aggregation*  nil {:duration 5 :field :time :aggr-fn :ssort :delay 10} rec)
                  state
                  [{:time 1}
                   {:time 2}
                   {:time 4}
                   {:time 4}
                   {:time 7}
                   {:time 30}
                   {:time 24}
                   {:time 41}]
                  [{:time 1}
                   {:time 2}
                   {:time 4}
                   {:time 4}
                   {:time 7}
                   {:time 24}
                   {:time 30}]))
  (let [[rec state] (recorder)]
    (test-actions (a/aggregation* nil {:duration 10 :field :time :aggr-fn :ssort :delay 20} rec)
                  state
                  [{:time 1}
                   {:time 10}
                   {:time 4}
                   {:time 9}
                   {:time 13}
                   {:time 41}]
                  [{:time 1}
                   {:time 4}
                   {:time 9}
                   {:time 10}
                   {:time 13}]))
  (let [[rec state] (recorder)]
    (test-actions (a/aggregation* nil {:duration 10 :field [:nested :a] :aggr-fn :ssort :nested? true} rec)
                  state
                  [{:time 1 :nested {:a 4}}
                   {:time 3 :nested {:a 1}}
                   {:time 7 :nested {:a -1}}
                   {:time 9 :nested {:a 10}}
                   {:time 11 :nested {:a 3}}]
                  [{:time 7 :nested {:a -1}}
                   {:time 3 :nested {:a 1}}
                   {:time 1 :nested {:a 4}}
                   {:time 9 :nested {:a 10}}])))

(deftest coll-increase*
  (let [[rec state] (recorder)]
    (test-actions (a/coll-increase* nil rec)
                  state
                  [[{:time 1 :metric 10}
                    {:time 2 :metric 20}
                    {:time 11 :metric 50}]
                   [{:time 14 :metric 60}
                    {:time 28 :metric 90}]]
                  [{:time 11 :metric 40}
                   {:time 28 :metric 30}]))
  (let [[rec state] (recorder)]
    (test-actions (a/coll-increase* nil rec)
                  state
                  [[{:time 1 :metric 10}
                    {:time 2 :metric 20}
                    {:time 11 :metric 1}]][])))

(deftest extract-test
  (let [[rec state] (recorder)]
    (test-actions (a/extract* nil :foo rec)
                  state
                  [{:foo {:time 1}}
                   {:foo {:time 2} :bar "a"}
                   {:foo nil}
                   {}]
                  [{:time 1}
                   {:time 2}])
    (test-actions (a/extract* nil [:foo :bar] rec)
                  state
                  [{:foo {:bar {:time 1}}}
                   {:foo {:bar {:time 2}} :bar "a"}
                   {:foo {:bar nil}}
                   {}]
                  [{:time 1}
                   {:time 2}])))

(deftest aggr-rate*-test
  (testing "no delay"
    (let [[rec state] (recorder)]
      (test-actions (a/aggregation* nil
                                    {:duration (time/s->ns 10)
                                     :aggr-fn :rate
                                     :delay 0}
                                    rec)
                    state
                    (time/events-time-s->ns
                     [{:time 0 :metric 10}
                      {:time 7 :metric 1}
                      {:time 11 :metric 3}
                      {:time 14 :metric 8}
                      {:time 19 :metric 1}
                      {:time 20 :metric 2}
                      {:time 21 :metric 2}
                      {:time 21 :metric 2000}
                      {:time 24 :metric 24}
                      {:time 22 :metric 24}
                      {:time 31 :metric 1}])
                    (time/events-time-s->ns
                     [{:time 7 :metric 1/5}
                      {:time 19 :metric 3/10}
                      {:time 24 :metric 1/2}])))))

(deftest percentiles-test
  (let [[rec state] (recorder)]
    (test-actions (a/percentiles* nil
                                  {:percentiles [0 0.5 0.99 1]
                                   :duration 10
                                   :nb-significant-digits 3}
                                  rec)
                  state
                  [{:time 1 :metric 100}
                   {:time 2 :metric 200}
                   {:time 2 :metric 200}
                   {:time 2 :metric 200}
                   {:time 2 :metric 200}
                   {:time 2 :metric 200}
                   {:time 4 :metric 800}
                   {:time 12 :metric 800}]
                  [{:time 12, :metric 100, :quantile 0}
                   {:time 12, :metric 200, :quantile 0.5}
                   {:time 12, :metric 800, :quantile 0.99}
                   {:time 12, :metric 800, :quantile 1}])))

(deftest to-string-test
  (let [[rec state] (recorder)]
    (test-actions (a/to-string* nil
                                [:foo :bar]
                                rec)
                  state
                  [{}
                   {:foo 1 :bar {:a {:b true}} :baz 3}]
                  [{:foo "" :bar ""}
                   {:foo "1" :bar "{:a {:b true}}" :baz 3}])
    (test-actions (a/to-string* nil
                                [:foo [:bar :a :b]]
                                rec)
                  state
                  [{}
                   {:foo 1 :bar {:a {:b true}} :baz 3}]
                  [{:foo "" :bar {:a {:b ""}}}
                   {:foo "1" :bar {:a {:b "true"}} :baz 3}])))

(deftest ratio-test
  (let [[rec state] (recorder)]
    (test-actions (a/aggregation* nil
                                  {:duration 10 :aggr-fn :ratio :delay 0
                                   :conditions [[:= :state "error"] [:true]]}
                                  rec)
                  state
                  [{:state "ok" :time 1}
                   {:state "ok" :time 2}
                   {:state "ok" :time 2}
                   {:state "error" :time 3}
                   {:state "ok" :time 4}
                   {:state "ok" :time 12}
                   {:state "error" :time 13}
                   {:state "ok" :time 19}
                   {:state "ok" :time 20}
                   {:state "ok" :time 21}
                   {:state "ok" :time 22}
                   {:state "error" :time 23}
                   {:state "error" :time 140}
                   {:state "ok" :time 141}
                   {:state "ok", :time 141}
                   {:state "error", :time 142}
                   {:state "ok" :time 1151}]
                  [{:state "ok", :time 4, :metric (/ 1 5)}
                   {:state "ok", :time 20, :metric (/ 1 4)}
                   {:state "error", :time 23, :metric (/ 1 3)}
                   {:state "error", :time 140, :metric 1}
                   {:state "error", :time 142, :metric 1/3}]))
  (let [[rec state] (recorder)]
    (test-actions (a/aggregation* nil
                                  {:duration 10 :aggr-fn :ratio :delay 0 :metric true
                                   :conditions [[:= :state "error"] [:true]]}
                                  rec)
                  state
                  [{:state "ok" :time 1 :metric 1}
                   {:state "ok" :time 2 :metric 1}
                   {:state "ok" :time 2 :metric 2}
                   {:state "error" :time 3 :metric 10}
                   {:state "ok" :time 4 :metric 3}
                   {:state "error" :time 12 :metric 2}
                   {:state "error" :time 13 :metric 1}
                   {:state "ok" :time 19 :metric 10}
                   {:state "ok" :time 20 :metric 3}
                   {:state "ok" :time 21 :metric 2}
                   {:state "ok" :time 22 :metric 10}
                   {:state "error" :time 2 :metric 13}
                   ]
                  [{:state "ok", :time 4, :metric 10/17}
                   {:state "ok", :time 20, :metric 3/16}]))
    (let [[rec state] (recorder)]
    (test-actions (a/aggregation* nil
                                  {:duration 10 :aggr-fn :ratio :delay 0
                                   :conditions [[:= :state "error"] [:= :state "warn"]]}
                                  rec)
                  state
                  [{:state "error" :time 1 :metric 1}
                   {:state "error" :time 2 :metric 1}
                   {:state "ok" :time 2 :metric 2}
                   {:state "error" :time 3 :metric 10}
                   {:state "ok" :time 4 :metric 3}
                   {:state "error" :time 12 :metric 2}
                   {:state "error" :time 13 :metric 1}
                   {:state "error" :time 19 :metric 10}
                   {:state "ok" :time 20 :metric 3}
                   {:state "ok" :time 21 :metric 2}
                   {:state "ok" :time 22 :metric 10}
                   {:state "error" :time 2 :metric 13}
                   ]
                  [{:state "ok", :time 4, :metric 0}
                   {:state "ok", :time 20, :metric 0}])))
