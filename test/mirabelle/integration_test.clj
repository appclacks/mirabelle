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
    (let [resp (http/get "http://localhost:5666/healthz"
                         {:as :json})]
      (is (= 200 (:status resp)))
      (is (= {:message "ok"}
             (:body resp)))))
  (testing "add-stream"
    (let [body (-> {:config (-> {:actions {:action :index :params [[:host]]}}
                                pr-str
                                b64/to-base64)}
                   json/generate-string)
          resp (http/post "http://localhost:5666/api/v1/stream/foo"
                          {:content-type :json
                           :as :json
                           :body body})]
      (is (= 200 (:status resp)))
      (is (= {:message "stream added"}
             (:body resp)))))
  (testing "push-event"
    (let [body (-> {:event {:host "foo" :time 3}}
                   json/generate-string)
          resp (http/put "http://localhost:5666/api/v1/stream/foo"
                         {:content-type :json
                          :as :json
                          :body body})]
      (is (= 200 (:status resp)))
      (is (= {:message "ok"}
             (:body resp))))
    (let [body (-> {:event {:host "foo"}}
                   json/generate-string)
          resp (http/put "http://localhost:5666/api/v1/stream/bar"
                         {:content-type :json
                          :throw-exceptions false
                          :as :json
                          :body body})]
      (is (= 404 (:status resp)))
      (is (= {:error "Stream :bar not found"}
             (json/parse-string (:body resp) true)))))
  (testing "search-index"
    (let [body (-> {:query (-> [:always-true]
                                pr-str
                                b64/to-base64)}
                   json/generate-string)
          resp (http/post "http://localhost:5666/api/v1/index/foo/search"
                          {:content-type :json
                           :throw-exceptions false
                           :as :json
                           :body body})]
      (is (= 200 (:status resp)))
      (is (= {:events [{:host "foo" :time 3}]}
             (:body resp))))
    (let [body (-> {:query (-> [:always-true]
                                pr-str
                                b64/to-base64)}
                   json/generate-string)
          resp (http/post "http://localhost:5666/api/v1/index/bar/search"
                          {:content-type :json
                           :throw-exceptions false
                           :as :json
                           :body body})]
      (is (= 404 (:status resp)))
      (is (= {:error "stream :bar not found"}
             (json/parse-string (:body resp) true)))))
  (testing "get-stream"
    (let [resp (http/get "http://localhost:5666/api/v1/stream/foo"
                         {:as :json})]
      (is (= 200 (:status resp)))
      (is (= {:config (-> {:actions {:action :index :params [[:host]]}}
                          pr-str
                          b64/to-base64)
              :current-time 3}
             (:body resp)))))
  (testing "list-streams"
    (let [resp (http/get "http://localhost:5666/api/v1/stream"
                         {:as :json})]
      (is (= 200 (:status resp)))
      (is (= {:streams ["foo"]}
             (:body resp)))))
  (testing "current-time"
    (let [resp (http/get "http://localhost:5666/api/v1/current-time"
                         {:as :json})]
      (is (= 200 (:status resp)))
      (is (= {:current-time 0}
             (:body resp)))))
  (testing "remove-stream"
    (let [resp (http/delete "http://localhost:5666/api/v1/stream/foo"
                            {:as :json})]
      (is (= 200 (:status resp)))
      (is (= {:message "stream removed"}
             (:body resp))))
    (let [resp (http/get "http://localhost:5666/api/v1/stream"
                         {:as :json})]
      (is (= 200 (:status resp)))
      (is (= {:streams []}
             (:body resp))))))
