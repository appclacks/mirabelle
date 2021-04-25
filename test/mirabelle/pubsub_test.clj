(ns mirabelle.pubsub-test
  (:require [clojure.test :refer :all]
            [com.stuartsierra.component :as component]
            [mirabelle.pubsub :as pubsub]))

(deftest pubsub-test
  (let [component (component/start (pubsub/map->PubSub {}))
        state (atom [])]
    (is (= {} @(:subscriptions component)))
    (let [id (pubsub/add component :foo (fn [event] (swap! state conj event)))]
      (is (= [:foo] (keys @(:subscriptions component))))
      (pubsub/publish! component :foo {:host "a"})
      (pubsub/publish! component :bar {:host "b"})
      (is (= [{:host "a"}] @state))
      (pubsub/publish! component :foo {:host "c"})
      (is (= [{:host "a"} {:host "c"}] @state))
      (pubsub/rm component :foo id)
      (is (= {:foo {}} @(:subscriptions component)))
      (pubsub/rm component :bar id)
      (is (= {:foo {} :bar nil} @(:subscriptions component))))))
