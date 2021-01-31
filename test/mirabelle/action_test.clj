(ns mirabelle.action-test
  (:require [clojure.test :refer :all]
            [mirabelle.action :as a]
            [mirabelle.time :as t]))

(defn recorder
  []
  (let [state (atom [])]
    [(fn [event]
       (swap! state conj event))
     state]))

(defn test-action
  [action state input expected]
  (reset! state [])
  (doseq [event input]
    (action event))
  (is (= expected @state)))

(deftest valid-condition?-test
  (are [condition] (a/valid-condition? condition)
    [:> :metric 10]
    [:regex :host "foo"]
    [:< :metric 10]
    [:= :host "foo"]
    [:nil? :metric]
    [:and
     [:= :host "foo"]
     [:not-nil? :metric]]
    [:or
     [:= :host "foo"]
     [:not-nil? :metric]])
  (are [condition] (not (a/valid-condition? condition))
    [[:> :metric 10]]
    [:?? :metric 10]
    [:= "host" "foo"]
    [:foo :metric]
    [:foo
     [:= :host "foo"]
     [:not-nil? :metric]]
    [[[:= :host "foo"]
      [:not-nil? :metric]]]))

(deftest where*-test
  (let [[rec state] (recorder)]
    (test-action (a/where* nil [:pos? :metric] rec)
                 state
                 [{:metric 10} {:metric 10}]
                 [{:metric 10} {:metric 10}])
    (test-action (a/where* nil [:pos? :metric] rec)
                 state
                 [{:metric -1} {:metric 30} {:metric 0}]
                 [{:metric 30}])
    (test-action (a/where* nil [:> :metric 20] rec)
                 state
                 [{:metric -1} {:metric 30} {:metric 0}]
                 [{:metric 30}])
    (test-action (a/where* nil [:> :metric 20] rec)
                 state
                 [{:metric -1} {:metric 30} {:metric 40}]
                 [{:metric 30} {:metric 40}])
    (test-action (a/where* nil
                           [:and
                            [:> :metric 20]
                            [:< :metric 40]]
                           rec)
                 state
                 [{:metric -1} {:metric 30} {:metric 31} {:metric 50}]
                 [{:metric 30} {:metric 31}])
    (test-action (a/where* nil
                           [:or
                            [:< :metric 20]
                            [:> :metric 40]]
                           rec)
                 state
                 [{:metric -1} {:metric 30} {:metric 31} {:metric 50}]
                 [{:metric -1} {:metric 50}])
    (test-action (a/where* nil
                           [:regex :host "foo.*"]
                           rec)
                 state
                 [{:host "bar" :metric 1}
                  {:host "foo" :metric 2}
                  {:host "foobar" :metric 2}
                  {:host "baz" :metric 3}]
                 [{:host "foo" :metric 2}
                  {:host "foobar" :metric 2}])))

(deftest increment*-test
  (let [[rec state] (recorder)]
    (test-action (a/increment* nil rec)
                 state
                 [{:metric 10} {:metric 11}]
                 [{:metric 11} {:metric 12}])))

(deftest decrement*-test
  (let [[rec state] (recorder)]
    (test-action (a/increment* nil rec)
                 state
                 [{:metric 10} {:metric 11}]
                 [{:metric 11} {:metric 12}])))

(deftest fixed-event-window*-test
  (let [[rec state] (recorder)]
    (test-action (a/fixed-event-window* nil 3 rec)
                 state
                 [{:metric 10} {:metric 11}]
                 [])
    (test-action (a/fixed-event-window* nil 3 rec)
                 state
                 [{:metric 10} {:metric 11} {:metric 12}
                  {:metric 13} {:metric 14} {:metric 15}
                  {:metric 16}]
                 [[{:metric 10} {:metric 11} {:metric 12}]
                  [{:metric 13} {:metric 14} {:metric 15}]])))

(deftest list-mean*-test
  (let [[rec state] (recorder)]
    (test-action (a/list-mean* nil rec)
                 state
                 [[{:metric 10} {:metric 12}]]
                 [{:metric 11}])
    (test-action (a/list-mean* nil rec)
                 state
                 [[{:metric 10}]]
                 [{:metric 10}])
    (test-action (a/list-mean* nil rec)
                 state
                 [[{:metric 10 :time 3 :host "foo"}
                   {:metric 20 :time 1 :host "bar"}
                   {:metric 30 :time 2 :host "baz"}]]
                 [{:metric 20 :time 3 :host "foo"}])))

(deftest sdo-test
  (let [[rec state] (recorder)]
    (test-action (a/sdo* nil rec)
                 state
                 [{:metric 10}]
                 [{:metric 10}])))

(deftest expired-test
  (let [[rec state] (recorder)]
    (test-action (a/expired* nil rec)
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
    (test-action (a/not-expired* nil rec)
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
    (test-action (a/cond-dt* nil [:> :metric 10] 10 rec)
                 state
                 [{:time 1 :metric 12}
                  {:time 4 :metric 12}
                  {:time 12 :metric 12}
                  {:time 22 :metric 13}
                  {:time 23 :metric 1}
                  {:time 25 :metric 11}
                  {:time 36 :metric 12}]
                 [{:time 12 :metric 12}
                  {:time 22 :metric 13}
                  {:time 36 :metric 12}])))

(deftest critical*-test
  (let [[rec state] (recorder)]
    (test-action (a/critical* nil  rec)
                 state
                 [{:state "ok" :metric 1}
                  {:state "critical" :metric 2}
                  {:state "critical" :metric 3}
                  {:state "ok" :metric 1}
                  {:state "critical" :metric 5}]
                 [{:state "critical" :metric 2}
                  {:state "critical" :metric 3}
                  {:state "critical" :metric 5}])))

(deftest default*-test
  (let [[rec state] (recorder)]
    (test-action (a/default* nil :state "ok" rec)
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
                  {:state "critical" :metric 6}])))

(deftest coalesce*-test
  (let [state (atom [])
        rec (fn [event] (swap! state conj event))
        action (a/coalesce* nil 5 [:host :service] rec)]
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
        action (a/coalesce* nil 5 [:host :service] rec)]
    (doseq [event [{:host "1" :service "foo" :metric 1 :time 0 :ttl 20}
                   {:host "1" :service "baz" :metric 1 :time 1 :ttl 20}
                   {:host "1" :service "bar" :metric 1 :time 12 :ttl 20}]]
      (action event))
    (is (= [#{{:host "1" :service "foo" :metric 1 :time 0 :ttl 20}
              {:host "1" :service "baz" :metric 1 :time 1 :ttl 20}
              {:host "1" :service "bar" :metric 1 :time 12 :ttl 20}}]
           (map set @state)))))

(deftest set-field*-test
  (let [[rec state] (recorder)]
    (test-action (a/set-field* nil :state "ok" rec)
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
                  {:state "ok" :metric 6}])))

(deftest list-max*-test
  (let [[rec state] (recorder)]
    (test-action (a/list-max* nil rec)
                 state
                 [[{:metric 1}
                   {:metric 10}
                   {}
                   {}
                   {:metric 5}]]
                 [{:metric 10}])))

(deftest list-min*-test
  (let [[rec state] (recorder)]
    (test-action (a/list-min* nil rec)
                 state
                 [[{:metric 1}
                   {:metric 10}
                   {}
                   {}
                   {:metric 5}]]
                 [{:metric 1}])))

(deftest list-rate*-test
  (let [[rec state] (recorder)]
    (test-action (a/list-rate* nil rec)
                 state
                 [[{:metric 1 :time 1}
                   {:metric 10 :time 2}
                   {:metric 4 :time 3}
                   {:metric 10 :time 1}
                   {:metric 5 :time 4}]]
                 [{:time 4 :metric (/ 30 3)}])
    (test-action (a/list-rate* nil rec)
                 state
                 [[{:metric 1 :time 0}
                   {:metric 1 :time 2}
                   {:metric 1 :time 3}
                   {:metric 1 :time 1}
                   {:metric 1 :time 10}]]
                 [{:time 10 :metric (/ 5 10)}])
    (test-action (a/list-rate* nil rec)
                 state
                 [[{:metric 1 :time 1}
                   {:metric 2 :time 2}
                   {:metric 1 :time 3}]]
                 [{:time 3 :metric 2}])))

(deftest sflatten*-test
  (let [[rec state] (recorder)]
    (test-action (a/sflatten* nil rec)
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
    (test-action (a/tag* nil "foo" rec)
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
    (test-action (a/tag* nil ["foo" "bar"] rec)
                 state
                 [{:metric 1 :time 1}
                  {:metric 2 :tags ["foo"]}
                  {:metric 10 :time 1 :tags ["bar"]}
                  {:metric 5 :time 4 :tags ["foo"]}]
                 [{:metric 1 :time 1 :tags ["foo" "bar"]}
                  {:metric 2 :tags ["foo" "bar"]}
                  {:metric 10 :time 1 :tags ["foo" "bar"]}
                  {:metric 5 :time 4 :tags ["foo" "bar"]}])))

