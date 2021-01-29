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
