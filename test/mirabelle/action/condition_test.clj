(ns mirabelle.action.condition-test
  (:require [clojure.test :refer :all]
            [mirabelle.action.condition :as cd]))

(deftest valid-condition?-test
  (are [condition] (cd/valid-condition? condition)
    [:> [:foo :metric] 10]
    [:> [:metric] 10]
    [:> [:foo :bar :metric] 10]
    [:> :metric 10]
    [:regex :host "foo"]
    [:< :metric 10]
    [:= :host "foo"]
    [:nil? :metric]
    [:and
     [:= :host "foo"]
     [:not-nil? :metric]]
    [:and
     [:or
      [:= :host "foo"]
      [:not-nil? :metric]]
     [:and
      [:= :service "bar"]
      [:> :time 1]]]
    [:or
     [:= :host "foo"]
     [:not-nil? :metric]])
  (are [condition] (not (cd/valid-condition? condition))
    [[:> :metric 10]]
    [= :service "bar"]
    [:?? :metric 10]
    [:= "host" "foo"]
    [:foo :metric]
    [:foo
     [:= :host "foo"]
     [:not-nil? :metric]]
    [[[:= :host "foo"]
      [:not-nil? :metric]]]))
