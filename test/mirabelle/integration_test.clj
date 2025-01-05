(ns mirabelle.integration-test
  (:require [cheshire.core :as json]
            [clojure.test :refer :all]
            [clj-http.client :as http]
            [mirabelle.b64 :as b64]
            [mirabelle.core :as core]))

(defn system-fixture
  [f]
  (core/start!)
  (f)
  (core/stop!))

(use-fixtures :once system-fixture)

(def basic {"authorization" (str "Basic " (b64/to-base64
                                           "foo:my-password"))})

(deftest http-test
  (testing "healthz"
    (let [resp (http/get "http://localhost:5558/healthz"
                         {:as :json
                          :headers basic})]
      (is (= 200 (:status resp)))
      (is (= {:message "ok"}
             (:body resp)))))
  (testing "add-stream"
    (let [body (-> {:config (-> {:actions {:action :debug}}
                                pr-str
                                b64/to-base64)}
                   json/generate-string)
          resp (http/post "http://localhost:5558/api/v1/stream/test-foo"
                          {:content-type :json
                           :as :json
                           :headers basic
                           :body body})]
      (is (= 200 (:status resp)))
      (is (= {:message "stream added"}
             (:body resp)))))
  (testing "push-event"
    (let [body (-> {:events [{:host "test-foo" :time 3}]}
                   json/generate-string)
          resp (http/put "http://localhost:5558/api/v1/stream/test-foo"
                         {:content-type :json
                          :as :json
                          :headers basic
                          :body body})]
      (is (= 200 (:status resp)))
      (is (= {:message "ok"}
             (:body resp))))
    (let [body (-> {:events [{:host "test-foo"}]}
                   json/generate-string)
          resp (http/put "http://localhost:5558/api/v1/stream/test-bar"
                         {:content-type :json
                          :throw-exceptions false
                          :headers basic
                          :as :json
                          :body body})]
      (is (= 404 (:status resp)))
      (is (= {:error "Stream :test-bar not found"}
             (json/parse-string (:body resp) true)))))
  (testing "get-stream"
    (let [resp (http/get "http://localhost:5558/api/v1/stream/test-foo"
                         {:as :json
                          :headers basic})]
      (is (= 200 (:status resp)))
      (is (= {:config (-> {:actions {:action :debug} :default false}
                          pr-str
                          b64/to-base64)}
             (:body resp)))))
  (testing "list-streams"
    (let [resp (http/get "http://localhost:5558/api/v1/stream"
                         {:as :json
                          :headers basic})
          streams (-> resp :body :streams set)]
      (is (= 200 (:status resp)))
      (is (streams "test-foo"))))
  (testing "remove-stream"
    (let [resp (http/delete "http://localhost:5558/api/v1/stream/test-foo"
                            {:as :json
                             :headers basic})]
      (is (= 200 (:status resp)))
      (is (= {:message "stream removed"}
             (:body resp))))
    (let [resp (http/get "http://localhost:5558/api/v1/stream"
                         {:as :json
                          :headers basic})
          streams (-> resp :body :streams set)]
      (is (= 200 (:status resp)))
      (is (not (streams "test-foo")))))
  (testing "not-found"
    (let [resp (http/delete "http://localhost:5558/api/v1/notfound"
                            {:as :json
                             :headers basic
                             :throw-exceptions false})]
      (is (= 404 (:status resp)))
      (is (= {:error "uri /api/v1/notfound not found for method delete"}
             (json/parse-string (:body resp) true)))))
  (testing "bad auth"
    (let [resp (http/get "http://localhost:5558/healthz"
                         {:as :json
                          :throw-exceptions false})]
      (is (= 401 (:status resp))))
    (let [resp (http/get "http://localhost:5558/healthz"
                         {:as :json
                          :headers {"authorization" "Basic"}
                          :throw-exceptions false})]
      (is (= 401 (:status resp))))
    (let [resp (http/get "http://localhost:5558/healthz"
                         {:as :json
                          :headers {"authorization" (b64/to-base64 "foo:")}
                          :throw-exceptions false})]
      (is (= 401 (:status resp))))))
