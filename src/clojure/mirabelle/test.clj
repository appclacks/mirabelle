(ns mirabelle.test
  (:require [clojure.data :as data]
            [clojure.string :as string]
            [com.stuartsierra.component :as component]
            [corbihttp.log :as log]
            [corbihttp.metric :as metric]
            [mirabelle.index :as index]
            [mirabelle.stream :as stream]))

(defn tap-error-message
  [base-msg r]
  (let [[expected-missing not-expected]
        (data/diff (:expected r) (:actual r))]
    (cond-> (format "%s\nExpected:\n\n%s\n\nActual:\n\n%s"
                    base-msg
                    (pr-str (:expected r))
                    (pr-str (:actual r)))
      expected-missing (str (format "\n\nExpected in the tap but missing:\n\n%s" (pr-str expected-missing)))
      not-expected (str (format "\n\nNot expected in the tap:\n\n%s" (pr-str not-expected))))))

(defn test-result->message
  "Takes results from tests, build an human readable message"
  [result]
  (let [header (if (zero? (count result))
                 "All tests successful"
                 (format "%d errors\n" (count result)))]
    (->> result
         (map (fn [r]
                (cond-> (format "Error in test %s\n%s\n"
                                (:test r)
                                (:message r))

                  ( = (:type r) :exception)
                  (str (:exception r) "\n" (mapv str (.getStackTrace ^Exception
                                                                     (:exception r))))

                  ( = (:type r) :tap)
                  (tap-error-message r))))
         (string/join "\n\n----\n\n")
         (str header))))

(defn launch-tests
  [{:keys [stream test]}]
  (let [tests (stream/read-edn-dirs (:directories test))
        streams (stream/read-edn-dirs (:directories stream))
        registry (metric/registry-component {})
        result (atom [])]
    (doseq [[test-name test-config] tests]
      (log/infof {} "launching test %s" test-name)
      (try
        (let [stream-handler (component/start
                              (stream/map->StreamHandler
                               {:streams-directories (:directories stream)
                                :custom-actions (:actions stream)
                                :registry registry
                                :test-mode? true
                                ;; dedicated index in test mode
                                :index (component/start (index/map->Index {}))}))
              ;; the context source stream is not important here
              ;; because we only want the tap
              tap (:tap (stream/context stream-handler :test))]
          (if-let [target (:target test-config)]
            (do (stream/add-stream stream-handler
                                   target
                                   (get streams target))
                (doseq [event (:input test-config)]
                  (stream/push! stream-handler event target)))
            (doseq [event (:input test-config)]
              (stream/push! stream-handler event :default)))
          (doseq [[tap-name expected] (:tap-results test-config)]
            (when-not (= expected (get @tap tap-name))
              (swap! result
                     conj
                     {:message (format "Invalid result for tap %s" tap-name)
                      :test test-name
                      :expected expected
                      :type :tap
                      :actual (get @tap tap-name)}))))
        (catch Exception e
          (swap! result
                 conj
                 {:message "Exception during test"
                  :exception e
                  :type :exception
                  :test test-name}))))
    (test-result->message @result)))
