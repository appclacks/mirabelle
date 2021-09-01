(ns mirabelle.core
  (:require [clojure.spec.alpha :as s]
            [com.stuartsierra.component :as component]
            [corbihttp.http :as corbihttp]
            [corbihttp.log :as log]
            [corbihttp.metric :as metric]
            [mirabelle.config :as config]
            [mirabelle.db.queue :as queue]
            [mirabelle.handler :as handler]
            [mirabelle.http :as http]
            [mirabelle.index :as index]
            [mirabelle.test :as test]
            [mirabelle.transport :as transport]
            [mirabelle.pubsub :as pubsub]
            [mirabelle.transport.tcp :as tcp]
            [mirabelle.transport.websocket :as websocket]
            [mirabelle.stream :as stream]
            [signal.handler :refer [with-handler]]
            [unilog.config :refer [start-logging!]])
  (:gen-class))

(defonce ^:redef system
  nil)

(defn build-system
  [{:keys [tcp stream http queue io websocket]}]
  (let [registry (metric/registry-component {})
        queue-component (if queue
                          (component/start (queue/map->ChroniqueQueue queue))
                          (queue/map->DummyQueue {}))
        index (component/start (index/map->Index {}))
        pubsub (component/start (pubsub/map->PubSub {}))]
    (component/system-map
     :registry registry
     :index index
     :pubsub pubsub
     :queue queue-component
     :websocket (-> (websocket/map->WebsocketServer websocket)
                    (component/using [:pubsub :registry]))
     :http (-> (corbihttp/map->Server {:config http
                                       :registry registry
                                       :chain-builder http/interceptor-chain})
               (component/using [:api-handler]))
     :stream-handler (stream/map->StreamHandler
                      {:streams-directories (:directories stream)
                       :io-directories (:directories io)
                       :custom-io (:custom io)
                       :custom-actions (:actions stream)
                       :index index
                       :queue queue-component
                       :registry registry
                       :pubsub pubsub})
     :index index
     :api-handler (-> (handler/map->Handler {})
                      (component/using [:stream-handler :registry]))
     :shared-event-executor (transport/event-executor
                             (:event-executor-size tcp))
     :tcp-server (-> (tcp/map->TCPServer tcp)
                     (component/using [:shared-event-executor
                                       :stream-handler
                                       :registry])))))

(defn init-system
  "Initialize system, dropping the previous state."
  [config]
  (let [sys (build-system config)]
    (alter-var-root #'system (constantly sys))))

(defn stop!
  "Stop the system."
  []
  (let [sys (component/stop-system system)]
    (alter-var-root #'system (constantly sys))))

(defn reload!
  []
  (stream/reload (:stream-handler system)))

(defn start!
  "Start the system."
  []
  (try
    (let [config (config/load-config)
          _ (start-logging! (:logging config))
          _ (init-system config)
          sys (component/start-system system)]
      (alter-var-root #'system (constantly sys)))
    (catch Exception e
      (log/error {} e "fail to start the system")
      (throw e))))

(defn compile!
  [args]
  (log/info {} "compiling streams")
  (when-not (= 3 (count args))
    (log/error {} "Invalid parameters")
    (System/exit 1))
  (let [src-dir (second args)
        dst-dir (nth args 2)]
    (when-not (s/valid? ::config/directory-spec src-dir)
      (log/error {} (format "%s is not a directory" src-dir))
      (System/exit 1))
    (when-not (s/valid? ::config/directory-spec dst-dir)
      (log/error {} (format "%s is not a directory" src-dir))
      (System/exit 1))
    (config/compile-config! src-dir dst-dir))
  (log/info {} "Streams successfully compiled"))

(defn test!
  []
  (log/info {} "launching tests")
  (let [config (config/load-config)
        _ (start-logging! (:logging config))
        test-result (test/launch-tests
                     config)]
    (println test-result)))

(defn -main
  "Starts the application"
  [& args]
  (when (seq args)
    (condp = (first args)
      "compile" (compile! args)
      "test" (test!)
      "compile-test" (do (compile! args)
                         (test!))
      "compile_test" (do (compile! args)
                         (test!)))

    (System/exit 0))
  (with-handler :term
    (log/info {} "SIGTERM, stopping")
    (stop!)
    (log/info {} "the system is stopped")
    (System/exit 0))

  (with-handler :hup
    (log/info {} "SIGHUP, reloading")
    (try
      (reload!)
      (catch Exception e
        (log/error {} e "fail to reload")
        ))
    (log/info {} "reloaded"))

  (with-handler :int
    (log/info {} "SIGINT, stopping")
    (stop!)
    (log/info {} "the system is stopped")
    (System/exit 0))
  (try (start!)
       (log/info {} "J'ai une santé de fer. Voilà quinze ans que je vis à la campagne : que je me couche avec le soleil, et que je me lève avec les poules.")
       (log/info {} "system started")
       (catch Exception e
         (log/error {} e)
         (System/exit 1))))
