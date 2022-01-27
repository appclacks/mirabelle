(ns mirabelle.math-test
  (:require [clojure.test :refer :all]
            [mirabelle.math :as math]))

(deftest max-event-test
  (is (nil? (math/max-event [])))
  (is (nil? (math/max-event [{} {}])))
  (is (= {:metric 1} (math/max-event [{:metric 1}])))
  (is (= {:metric 4} (math/max-event [{:metric 1} {:metric nil} {} {:metric 4}])))
  (is (= {:metric 4} (math/max-event [{:metric nil} {:metric nil} {} {:metric 4}]))))

(deftest min-event-test
  (is (nil? (math/min-event [])))
  (is (nil? (math/min-event [{} {}])))
  (is (= {:metric 1} (math/min-event [{:metric 1}])))
  (is (= {:metric 1} (math/min-event [{:metric 1} {:metric nil} {} {:metric 4}])))
  (is (= {:metric 4} (math/min-event [{:metric nil} {:metric nil} {} {:metric 4}]))))

(deftest rate-test
  (is (nil? (math/rate [])))
  (is (= {:metric 1 :time 1} (math/rate [{:metric 1 :time 1}])))
  (is (= {:metric 2 :time 1} (math/rate [{:metric 1 :time 1}
                                         {:metric 1 :time 1}])))
  (is (= {:metric 11 :time 1} (math/rate [{:metric 1 :time 1}
                                         {:metric 10 :time 1}])))
  (is (= {:metric (/ 3 9) :time 10}
         (math/rate [{:metric 1 :time 2}
                     {:metric 1 :time 1}
                     {:metric 1 :time 10}]))))

;; Copyright Riemann authors (riemann.io), thanks to them!
(deftest sorted-sample-extract-test
  (are [es e] (= (math/sorted-sample-extract es [0 0.5 1]) e)
    []
    []

    [{:metric nil}]
    []

    [{:metric 1}]
    [{:metric 1} {:metric 1} {:metric 1}]

    [{:metric 2} {:metric 1}]
    [{:metric 1} {:metric 2} {:metric 2}]

    [{:metric 3} {:metric 1} {:metric 2}]
    [{:metric 1} {:metric 2} {:metric 3}]

    [{:metric 6} {:metric 1} {:metric 2} {:metric 1} {:metric 1}]
    [{:metric 1} {:metric 1} {:metric 6}]))

;; Copyright Riemann authors (riemann.io), thanks to them!
(deftest sorted-sample-test
  (are [es e] (= (math/sorted-sample es [0 0.5 1]) e)
    []
    []

    [{:metric nil}]
    []

    [{:metric 1 :service "foo"}]
    [{:metric 1 :service "foo" :quantile "0"} {:metric 1 :service "foo" :quantile "0.5"} {:metric 1 :service "foo" :quantile "1"}]

    [{:metric 2} {:metric 1}]
    [{:metric 1 :quantile "0"} {:metric 2 :quantile "0.5"} {:metric 2 :quantile "1"}]

    [{:metric 3} {:metric 1} {:metric 2}]
    [{:metric 1 :quantile "0"} {:metric 2 :quantile "0.5"} {:metric 3 :quantile "1"}]

    [{:metric 6} {:metric 1} {:metric 2} {:metric 1} {:metric 1}]
    [{:metric 1 :quantile "0"} {:metric 1 :quantile "0.5"} {:metric 6 :quantile "1"}]))

(deftest extremum-n-test
  (are [events expected] (= (math/extremum-n 3 > events) expected)
    [{:metric 1} {:metric 2} {:metric 4}]
    [{:metric 4} {:metric 2} {:metric 1}]

    [{:metric 10} {:metric 2} {:metric 4} {:metric 2}]
    [{:metric 10} {:metric 4} {:metric 2}])

  (are [events expected] (= (math/extremum-n 3 < events) expected)
    [{:metric 1} {:metric 2} {:metric 4}]
    [{:metric 1} {:metric 2} {:metric 4}]

    [{:metric 10} {:metric 2} {:metric 4} {:metric 1}]
    [{:metric 1} {:metric 2} {:metric 4}]))
