(ns mirabelle.stream-test
  (:require [clojure.test :refer :all]
            [mirabelle.action :as a]
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
