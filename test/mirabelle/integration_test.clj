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

(deftest http-test
  (testing "healthz"
    (let [resp (http/get "http://localhost:5558/healthz"
                         {:as :json})]
      (is (= 200 (:status resp)))
      (is (= {:message "ok"}
             (:body resp)))))
  (testing "add-stream"
    (let [body (-> {:config (-> {:actions {:action :index :params [[:host]]}}
                                pr-str
                                b64/to-base64)}
                   json/generate-string)
          resp (http/post "http://localhost:5558/api/v1/stream/test-foo"
                          {:content-type :json
                           :as :json
                           :body body})]
      (is (= 200 (:status resp)))
      (is (= {:message "stream added"}
             (:body resp)))))
  (testing "push-event"
    (let [body (-> {:event {:host "test-foo" :time 3}}
                   json/generate-string)
          resp (http/put "http://localhost:5558/api/v1/stream/test-foo"
                         {:content-type :json
                          :as :json
                          :body body})]
      (is (= 200 (:status resp)))
      (is (= {:message "ok"}
             (:body resp))))
    (let [body (-> {:event {:host "test-foo"}}
                   json/generate-string)
          resp (http/put "http://localhost:5558/api/v1/stream/test-bar"
                         {:content-type :json
                          :throw-exceptions false
                          :as :json
                          :body body})]
      (is (= 404 (:status resp)))
      (is (= {:error "Stream :test-bar not found"}
             (json/parse-string (:body resp) true)))))
  (testing "search-index"
    (let [body (-> {:query (-> [:always-true]
                                pr-str
                                b64/to-base64)}
                   json/generate-string)
          resp (http/post "http://localhost:5558/api/v1/index/test-foo/search"
                          {:content-type :json
                           :throw-exceptions false
                           :as :json
                           :body body})]
      (is (= 200 (:status resp)))
      (is (= {:events [{:host "test-foo" :time 3}]}
             (:body resp))))
    (let [body (-> {:query (-> [:always-true]
                                pr-str
                                b64/to-base64)}
                   json/generate-string)
          resp (http/post "http://localhost:5558/api/v1/index/test-bar/search"
                          {:content-type :json
                           :throw-exceptions false
                           :as :json
                           :body body})]
      (is (= 404 (:status resp)))
      (is (= {:error "stream :test-bar not found"}
             (json/parse-string (:body resp) true)))))
  (testing "get-stream"
    (let [resp (http/get "http://localhost:5558/api/v1/stream/test-foo"
                         {:as :json})]
      (is (= 200 (:status resp)))
      (is (= {:config (-> {:actions {:action :index :params [[:host]]} :default false}
                          pr-str
                          b64/to-base64)
              :current-time 3}
             (:body resp)))))
  (testing "list-streams"
    (let [resp (http/get "http://localhost:5558/api/v1/stream"
                         {:as :json})
          streams (-> resp :body :streams set)]
      (is (= 200 (:status resp)))
      (is (streams "test-foo"))))
  (testing "current-time"
    (let [resp (http/get "http://localhost:5558/api/v1/index/test-foo/current-time"
                         {:as :json})]
      (is (= 200 (:status resp)))
      (is (= {:current-time 3}
             (:body resp)))))
  (testing "remove-stream"
    (let [resp (http/delete "http://localhost:5558/api/v1/stream/test-foo"
                            {:as :json})]
      (is (= 200 (:status resp)))
      (is (= {:message "stream removed"}
             (:body resp))))
    (let [resp (http/get "http://localhost:5558/api/v1/stream"
                         {:as :json})
          streams (-> resp :body :streams set)]
      (is (= 200 (:status resp)))
      (is (not (streams "test-foo")))))
  (testing "not-found"
    (let [resp (http/delete "http://localhost:5558/api/v1/notfound"
                            {:as :json
                             :throw-exceptions false})]
      (is (= 404 (:status resp)))
      (is (= {:error "not found"}
             (json/parse-string (:body resp) true)))))
  (testing "search-index: wring parameter"
    (let [body (-> {:query nil}
                   json/generate-string)
          resp (http/post "http://localhost:5558/api/v1/index/test-foo/search"
                          {:content-type :json
                           :throw-exceptions false
                           :as :json
                           :body body})]
      (is (= 400 (:status resp)))
      (is (= {:error "field query is incorrect"}
             (json/parse-string (:body resp) true))))))
