(ns mirabelle.java.engine-test
  (:require [clojure.test :refer :all]
            [mirabelle.db.memtable :as memtable])
  (:import fr.mcorbin.mirabelle.memtable.Engine
           fr.mcorbin.mirabelle.memtable.Serie))

(deftest test-add-cleanup
  (let [engine ^Engine (Engine. 60 30)
        serie ^Serie (memtable/->serie "cpu" {"host" "mcorbin.fr"})
        ]
    (.add engine 1 serie 1)
    (is (= (.valuesFor engine serie) [1]))
    (.add engine 59 serie 2)
    (is (= (.valuesFor engine serie) [1 2]))

    (.add engine 62 serie 3)
    (is (= (.valuesFor engine serie) [2 3]))
    (.add engine 62 serie 4)
    (is (= (.valuesFor engine serie) [2 4]))
    (.add engine 70 serie 5)
    (.add engine 95 serie 6)
    (.add engine 96 serie 7)
    (.add engine 121 serie 8)
    (is (= (.valuesFor engine serie) [6 7 8]))

    (.add engine 300 serie 7)
    (is (= (.valuesFor engine serie) [7]))))

