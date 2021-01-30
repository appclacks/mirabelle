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
  (is (= @state expected)))

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
                 [{:metric -1} {:metric 50}])))

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

(deftest mean*-test
  (let [[rec state] (recorder)]
    (test-action (a/mean* nil rec)
                 state
                 [[{:metric 10} {:metric 12}]]
                 [{:metric 11}])
    (test-action (a/mean* nil rec)
                 state
                 [[{:metric 10}]]
                 [{:metric 10}])
    (test-action (a/mean* nil rec)
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
                  {:time 1}
                  {:time (t/now)}]
                 [{:state "expired"}
                  {:time 1}])))

(deftest not-expired-test
  (let [[rec state] (recorder)
        current-time (t/now)]
    (test-action (a/not-expired* nil rec)
                 state
                 [{:state "expired"}
                  {:state "ok"}
                  {:time 1}
                  {:time current-time}]
                 [{:state "ok"}
                  {:time current-time}])))

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
