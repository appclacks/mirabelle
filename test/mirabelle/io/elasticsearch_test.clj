(ns mirabelle.io.elasticsearch-test
  (:require [clojure.test :refer :all]
            [mirabelle.io.elasticsearch :as es])
  (:import java.time.format.DateTimeFormatter))

(def formatter (DateTimeFormatter/ofPattern "yyyy-MM-dd"))

(deftest format-event-test
  (is (= (str "{\"index\":{\"_index\":\"foo-1970-01-01\"}}\n"
              "{\"@timestamp\":\"1970-01-01T00:00:01Z\"}"
              "\n")
         (es/format-event {:default-index "foo"
                           :default-index-formatter formatter}
                          {:time 1})))
  (is (= (str "{\"index\":{\"_index\":\"foo-1970-01-01\"}}\n"
              "{\"service\":\"foo\",\"@timestamp\":\"1970-01-01T00:00:01Z\"}"
              "\n")
         (es/format-event {:default-index "foo"
                           :default-index-formatter formatter}
                          {:time 1 :service "foo"})))
  (is (= (str "{\"index\":{\"_index\":\"foo\"}}\n"
              "{\"service\":\"foo\",\"@timestamp\":\"1970-01-01T00:00:01Z\"}"
              "\n")
         (es/format-event {:default-index "foo"}
                          {:time 1 :service "foo"}))))
