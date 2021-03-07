(ns mirabelle.test-test
  (:require [clojure.java.io :as io]
            [clojure.test :refer :all]
            mirabelle.action
            [mirabelle.test :as t]))

(deftest launch-tests-test
  (let [result (t/launch-tests
                {:stream {:actions {:custom mirabelle.action/where*}
                          :directories [(.getPath (io/resource "test/successful/streams"))]}
                 :test {:directories [(.getPath (io/resource "test/successful/tests"))]}})]
    (is (= result "All tests successful")))
  (let [result (t/launch-tests
                {:stream {:actions {:custom mirabelle.action/where*}
                          :directories [(.getPath (io/resource "test/failed/streams"))]}
                 :test {:directories [(.getPath (io/resource "test/failed/tests"))]}})]
    (is (.contains result "1 errors"))
    (is (.contains result "in test :t2"))))
