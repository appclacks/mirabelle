(ns mirabelle.test
  (:require [clojure.string :as string]
            [com.stuartsierra.component :as component]
            [corbihttp.log :as log]
            [corbihttp.metric :as metric]
            [mirabelle.index :as index]
            [mirabelle.stream :as stream]))

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
                  (str (:exception r) "\n" (map str (.getStackTrace (:exception r))))

                  ( = (:type r) :tap)
                  (str (format "expected: %s\nactual: %s"
                               (pr-str (:expected r))
                               (pr-str (:actual r)))))))
         (string/join "\n----\n")
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
                                :index (index/map->Index {})}))
              ;; the context source stream is not important here
              ;; because we only want the tap
              tap (:tap (stream/context stream-handler :test))]
          (if-let [target (:target test-config)]
            (do (stream/add-dynamic-stream stream-handler
                                           target
                                           (get streams target))
                (doseq [event (:input test-config)]
                  (stream/push! stream-handler event target)))
            (doseq [event (:input test-config)]
              (stream/push! stream-handler event :streaming)))
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
