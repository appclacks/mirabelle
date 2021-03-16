(ns mirabelle.io.pagerduty-test
  (:require [clojure.test :refer :all]
            [com.stuartsierra.component :as component]
            [mirabelle.io.pagerduty :as pd]))

(deftest event+keys->str-test
  (is (= "foo - bar - baz"
         (pd/event+keys->str {:host "foo" :service "bar" :foo "baz"}
                             [:host :service :foo]
                             " - ")))
  (is (= "foo-bar"
         (pd/event+keys->str {:host "foo" :service "bar" :foo "baz"}
                             [:host :service]"-"))))

(deftest format-event-test
  (is (= {:summary "foo - bar"
          :source "foo"
          :severity "critical"
          :timestamp "1970-01-01T01:00:00.001+01:00"
          :custom_details
          {:host "foo" :service "bar" :state "critical" :time 1}}
         (pd/format-event {:source-key :host
                           :summary-keys [:host :service]}
                          {:host "foo" :service "bar" :state "critical" :time 1})))
  (is (= {:summary "foo - bar"
          :source "bar"
          :severity "critical"
          :timestamp "1970-01-01T01:00:00.001+01:00"
          :custom_details
          {:host "foo" :service "bar" :time 1}}
         (pd/format-event {:source-key :service
                           :summary-keys [:host :service]}
                          {:host "foo" :service "bar" :time 1})))
  (is (= {:summary "foo - bar"
          :source "bar"
          :severity "info"
          :timestamp "1970-01-01T01:00:00.001+01:00"
          :custom_details
          {:host "foo" :service "bar" :time 1 :state "ok"}}
         (pd/format-event {:source-key :service
                           :summary-keys [:host :service]}
                          {:host "foo" :service "bar" :time 1 :state "ok"}))))

(deftest request-body-test
  (is (= {:routing_key "mcorbin"
          :event_action :trigger
          :payload {:summary "foo - bar"
                    :source "foo"
                    :severity "critical"
                    :timestamp "1970-01-01T01:00:00.001+01:00"
                    :custom_details
                    {:host "foo" :service "bar" :state "critical" :time 1}}
          :dedup_key "foo-bar"}
         (pd/request-body {:service-key "mcorbin"
                           :dedup-keys [:host :service]
                           :source-key :host
                           :summary-keys [:host :service]}
                          :trigger
                          {:host "foo" :service "bar" :state "critical" :time 1})))
  (is (= {:routing_key "mcorbin"
          :event_action :trigger
          :payload {:summary "foo - bar"
                    :source "foo"
                    :severity "critical"
                    :timestamp "1970-01-01T01:00:00.001+01:00"
                    :custom_details
                    {:host "foo" :service "bar" :state "critical" :time 1}}}
         (pd/request-body {:service-key "mcorbin"
                           :source-key :host
                           :summary-keys [:host :service]}
                          :trigger
                          {:host "foo" :service "bar" :state "critical" :time 1}))))

(deftest start-stop-test
  (testing "valid specs"
    (let [client (component/start
                  (pd/map->Pagerduty {:service-key "foo"
                                      :http-options {}
                                      :source-key :service
                                      :summary-keys [:host :service :state :metric]
                                      :dedup-keys [:host :service]}))]
      (component/stop client))
    (let [client (component/start
                  (pd/map->Pagerduty {:service-key "foo"
                                      :source-key :service
                                      :summary-keys [:host :service :state :metric]
                                      :dedup-keys [:host :service]}))]
      (component/stop client)))
  (testing "invalid specs"
    (is (thrown-with-msg?
         Exception
         #"Invalid spec"
         (component/start
          (pd/map->Pagerduty {:service-key "foo"
                              :http-options {}
                              :source-key "service"
                              :summary-keys [:host :service :state :metric]
                              :dedup-keys [:host :service]}))))
    (is (thrown-with-msg?
         Exception
         #"Invalid spec"
         (component/start
          (pd/map->Pagerduty {:service-key "foo"
                              :http-options {}
                              :source-key :service
                              :summary-keys [:host :service :state :metric]
                              :dedup-keys []}))))
    (is (thrown-with-msg?
         Exception
         #"Invalid spec"
         (component/start
          (pd/map->Pagerduty {:service-key "foo"
                              :http-options {}
                              :source-key :service
                              :summary-keys []
                              :dedup-keys [:host]}))))))
