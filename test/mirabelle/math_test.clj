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
  (is (= {:metric 11 :time 1} (math/rate [{:metric 1 :time 1}
                                         {:metric 10 :time 1}])))
  (is (= {:metric (/ 3 9) :time 10}
         (math/rate [{:metric 1 :time 2}
                     {:metric 1 :time 1}
                     {:metric 1 :time 10}]))))
