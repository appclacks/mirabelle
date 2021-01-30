(ns mirabelle.event-test
  (:require [clojure.test :refer :all]
            [mirabelle.event :as e]
            [mirabelle.time :as t]))

(deftest expired?-test
  (are [event] (e/expired? event)
    {:time 1}
    {:state "expired"}
    {:time (- (t/now) 20) :ttl 10})
  (are [event] (not (e/expired? event))
    {:time (t/now)}
    {:state "ok"}
    {:time (- (t/now) 20) :ttl 40}))
