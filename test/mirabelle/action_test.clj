(ns mirabelle.action-test
  (:require [clojure.test :refer :all]
            [mirabelle.action :as a]))

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

(deftest keep-non-discarded-events-test
  (is (nil? (a/keep-non-discarded-events {:tags ["discard"]})))
  (is (= {:tags ["foo"]} (a/keep-non-discarded-events {:tags ["foo"]})))
  (is (= [{:tags ["foo"]}] (a/keep-non-discarded-events [{:tags ["foo"]}])))
  (is (nil? (a/keep-non-discarded-events [{:tags ["discard"]}])))
  (is (= [{:tags ["ok"]}]
         (a/keep-non-discarded-events [{:tags ["discard"]} {:tags ["ok"]}]))))

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
                  {:host "foobar" :metric 2}])
    (test-action (a/where* nil [:contains :tags "foo"] rec)
                 state
                 [{:metric 10} {:metric 10 :tags ["foo"]} {:tags []}]
                 [{:metric 10 :tags ["foo"]}])
    (test-action (a/where* nil [:absent :tags "foo"] rec)
                 state
                 [{:metric 10} {:metric 10 :tags ["foo"]} {:tags []}]
                 [{:metric 10} {:tags []}])))

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

(deftest coll-mean*-test
  (let [[rec state] (recorder)]
    (test-action (a/coll-mean* nil rec)
                 state
                 [[{:metric 10}]]
                 [{:metric 10}])
    (test-action (a/coll-mean* nil rec)
                 state
                 [[{:metric 10} {:metric 12}]]
                 [{:metric 11}])
    (test-action (a/coll-mean* nil rec)
                 state
                 [[{:metric 10}]]
                 [{:metric 10}])
    (test-action (a/coll-mean* nil rec)
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
    (test-action (a/critical* nil rec)
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
    (doseq [event [{:host "1" :service "foo" :metric 1 :time 0 :ttl 10}
                   {:host "1" :service "bar" :metric 1 :time 5 :ttl 10}]]
      (action event))
    (is (= [#{{:host "1", :service "foo", :metric 1, :time 0, :ttl 10}
              {:host "1", :service "bar", :metric 1, :time 5, :ttl 10}}]
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

(deftest with*-test
  (let [[rec state] (recorder)]
    (test-action (a/with* nil {:state "ok"} rec)
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

(deftest coll-max*-test
  (let [[rec state] (recorder)]
    (test-action (a/coll-max* nil rec)
                 state
                 [[{:metric 1}
                   {:metric 10}
                   {}
                   {}
                   {:metric 5}]]
                 [{:metric 10}])))

(deftest coll-min*-test
  (let [[rec state] (recorder)]
    (test-action (a/coll-min* nil rec)
                 state
                 [[{:metric 1}
                   {:metric 10}
                   {}
                   {}
                   {:metric 5}]]
                 [{:metric 1}])))

(deftest coll-rate*-test
  (let [[rec state] (recorder)]
    (test-action (a/coll-rate* nil rec)
                 state
                 [[{:metric 1 :time 1}]]
                 [{:metric 1 :time 1}])
    (test-action (a/coll-rate* nil rec)
                 state
                 [[{:metric 1 :time 1}
                   {:metric 10 :time 2}
                   {:metric 4 :time 3}
                   {:metric 10 :time 1}
                   {:metric 5 :time 4}]]
                 [{:time 4 :metric (/ 30 3)}])
    (test-action (a/coll-rate* nil rec)
                 state
                 [[{:metric 1 :time 0}
                   {:metric 1 :time 2}
                   {:metric 1 :time 3}
                   {:metric 1 :time 1}
                   {:metric 1 :time 10}]]
                 [{:time 10 :metric (/ 5 10)}])
    (test-action (a/coll-rate* nil rec)
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

(deftest untag*-test
  (let [[rec state] (recorder)]
    (test-action (a/untag* nil "foo" rec)
                 state
                 [{:metric 1 :time 1}
                  {:metric 2 :tags ["foo"]}
                  {:metric 10 :time 1 :tags ["bar"]}
                  {:metric 5 :time 4 :tags ["foo" "bar"]}]
                 [{:metric 1 :time 1 :tags []}
                  {:metric 2 :tags []}
                  {:metric 10 :time 1 :tags ["bar"]}
                  {:metric 5 :time 4 :tags ["bar"]}])
    (test-action (a/untag* nil ["foo" "bar"] rec)
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
    (test-action (a/ddt* nil false rec)
                 state
                 [{:metric 1 :time 1}
                  {:metric 10 :time 4}
                  {:metric 11 :time 7}
                  {:metric 8 :time 10}
                  ]
                 [{:metric (/ 9 3) :time 4}
                  {:metric (/ 1 3) :time 7}
                  {:metric -1 :time 10}])
    (test-action (a/ddt* nil true rec)
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
    (test-action (a/scale* nil 10 rec)
                 state
                 [{:metric 1 :time 1}
                  {:metric 10 :time 4}
                  ]
                 [{:metric 10 :time 1}
                  {:metric 100 :time 4}])))

(deftest split*-test
  (let [[rec state] (recorder)]
    (test-action (a/split* nil
                           [[:= :state "critical"]]
                           rec)
                 state
                 [{:metric 1 :time 1 :state "ok"}
                  {:metric 1 :time 1 :state "warning"}
                  {:metric 1 :time 1 :state "critical"}]
                 [{:metric 1 :time 1 :state "critical"}]))
  (let [[rec state] (recorder)
        [rec2 state2] (recorder)]
    (test-action (a/split* nil
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
    (test-action (a/throttle* nil
                              5
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
                  {:metric 1 :time 18 :state "ok"}
                  ]
                 [{:metric 1 :time 0 :state "ok"}
                  {:metric 1 :time 5 :state "ok"}
                  {:metric 1 :time 12 :state "ok"}
                  {:metric 1 :time 18 :state "ok"}])))

(deftest fixed-time-window*-test
  (let [[rec state] (recorder)]
    (test-action (a/fixed-time-window* nil
                                       5
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
                  []
                  []
                  [{:metric 1 :time 29 :state "ok"}]])))

(deftest moving-event-window*-test
  (let [[rec state] (recorder)]
    (test-action (a/moving-event-window* nil
                                         5
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
    (test-action (a/ewma-timeless* nil
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
    (test-action (a/ewma-timeless* nil
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
    (test-action (a/ewma-timeless* nil
                                   0.5
                                   rec)
                 state
                 [{:metric 1 :time 0 :state "ok"}
                  {:metric 1 :time 1 :state "ok"}
                  {:metric 1 :time 2 :state "ok"}]
                 [{:metric 0.5 :time 0 :state "ok"}
                  {:metric 0.75 :time 1 :state "ok"}
                  {:metric 0.875 :time 2 :state "ok"}])))

(deftest warning*-test
  (let [[rec state] (recorder)]
    (test-action (a/warning* nil rec)
                 state
                 [{:state "ok" :metric 1}
                  {:state "warning" :metric 2}
                  {:state "warning" :metric 3}
                  {:state "critical" :metric 1}
                  {:state "warning" :metric 5}]
                 [{:state "warning" :metric 2}
                  {:state "warning" :metric 3}
                  {:state "warning" :metric 5}])))

(deftest over*-test
  (let [[rec state] (recorder)]
    (test-action (a/over* nil 10 rec)
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
    (test-action (a/under* nil 10 rec)
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
    (test-action (a/changed* nil :state "ok" rec)
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
                  {:metric 6 :state "critical"}])))

(deftest project*-test
  (let [[rec state] (recorder)]
    (test-action (a/project* nil
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
    (test-action (a/coll-count* nil
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
    (test-action (a/coll-count* nil
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
    (test-action (a/sdissoc* nil
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
    (test-action (a/sdissoc* nil
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
                  {:metric 3}])))

(deftest percentiles*-test
  (let [[rec state] (recorder)]
    (test-action (a/percentiles* nil
                                 [0 0.5 1]
                                 rec)
                 state
                 [[{:metric 3} {:metric 1} {:metric 2}]]
                 [{:metric 1 :quantile "0"}
                  {:metric 2 :quantile "0.5"}
                  {:metric 3 :quantile "1"}])))

(deftest tagged-all*-test
  (let [[rec state] (recorder)]
    (test-action (a/tagged-all* nil
                                "foo"
                                rec)
                 state
                 [{:metric 3} {:metric 1 :tags ["a"]} {:metric 2 :tags ["foo"]}]
                 [{:metric 2 :tags ["foo"]}])
    (test-action (a/tagged-all* nil
                                ["foo" "bar"]
                                rec)
                 state
                 [{:metric 3}
                  {:metric 1 :tags ["a"]}
                  {:metric 2 :tags ["foo"]}
                  {:metric 3 :tags ["foo" "bar"]}]
                 [{:metric 3 :tags ["foo" "bar"]}])))

(deftest json-fields*-test
  (let [[rec state] (recorder)]
    (test-action (a/json-fields* nil
                                 [:foo]
                                 rec)
                 state
                 [{:foo "{\"bar\": \"baz\"}"}]
                 [{:foo {:bar "baz"}}])
    (test-action (a/json-fields* nil
                                 [:foo :bar]
                                 rec)
                 state
                 [{:foo "{\"bar\": \"baz\"}"
                   :host "h"
                   :bar "{\"a\": \"b\"}"}]
                 [{:foo {:bar "baz"}
                   :host "h"
                   :bar {:a "b"}}])
    (test-action (a/json-fields* nil
                                 [:foo]
                                 rec)
                 state
                 [{:host "aa"}]
                 [{:host "aa"}])))

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
                          :children [{:action :increment
                                      :children nil}]}}}
         (a/streams
          (a/stream
           {:name :foo}
           (a/increment)))))
  (is (= {:foo {:actions {:action :sdo
                          :children [{:action :increment
                                      :children nil}]}}
          :bar {:actions {:action :sdo
                          :children [{:action :decrement
                                      :children nil}
                                     {:action :increment
                                      :children nil}]}}}
         (a/streams
          (a/stream
           {:name :foo}
           (a/increment))
          (a/stream
           {:name :bar}
           (a/decrement)
           (a/increment))))))
