(ns mirabelle.output.file-test
  (:require [clojure.test :refer :all]
            [mirabelle.output.file :as file]))

(deftest format-path-test
  (is (= "foo-bar-2023-12-23"
         ((file/format-path "foo-%s" [:service] "yyyy-MM-dd")
          {:service "bar" :time 1703331988})))
  (is (= "foo-bar-baz-2023-12-23"
         ((file/format-path "foo-%s-%s" [:service [:labels :name]] "yyyy-MM-dd")
          {:service "bar" :time 1703331988 :labels {:name "baz"}})))
  (is (= "foo-bar-baz"
         ((file/format-path "foo-%s-%s" [:service [:labels :name]] nil)
          {:service "bar" :time 1703331988 :labels {:name "baz"}})))
  (is (= "foo-2023-12-23"
         ((file/format-path "foo" nil "yyyy-MM-dd")
          {:service "bar" :time 1703331988 :labels {:name "baz"}})))
  (is (= "foo"
         ((file/format-path "foo" nil nil)
          {:service "bar" :time 1703331988 :labels {:name "baz"}}))))
