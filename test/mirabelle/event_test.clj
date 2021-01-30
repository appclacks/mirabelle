(ns mirabelle.event-test
  (:require [clojure.test :refer :all]
            [mirabelle.event :as e]
            [mirabelle.time :as t]))

(deftest expired?-test
  (are [event] (e/expired? 300 event)
    {:time 1}
    {:state "expired"}
    {:time 280 :ttl 10})
  (are [event] (not (e/expired? 300 event))
    {:time 250}
    {:state "ok"}
    {:time 200 :ttl 110}))
