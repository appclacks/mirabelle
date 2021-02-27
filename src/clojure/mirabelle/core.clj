(ns mirabelle.core
  (:require [com.stuartsierra.component :as component]
            [corbihttp.http :as corbihttp]
            [corbihttp.log :as log]
            [corbihttp.metric :as metric]
            [mirabelle.config :as config]
            [mirabelle.db.queue :as queue]
            [mirabelle.db.memtable :as memtable]
            [mirabelle.handler :as handler]
            [mirabelle.http :as http]
            [mirabelle.transport :as transport]
            [mirabelle.transport.tcp :as tcp]
            [mirabelle.stream :as stream]
            [signal.handler :refer [with-handler]]
            [unilog.config :refer [start-logging!]])
  (:import mirabelle.stream.StreamHandler)
  (:gen-class))

(defonce ^:redef system
  nil)

(defn build-system
  [{:keys [tcp stream http queue]}]
  (let [registry (metric/registry-component {})
        memtable-engine (component/start (memtable/map->MemtableEngine {:memtable-max-ttl 3600
                                                                        :memtable-cleanup-duration 1000}))
        queue-component (component/start (queue/map->ChroniqueQueue queue))]
    (component/system-map
     :registry registry
     :http (-> (corbihttp/map->Server {:config http})
               (component/using [:handler]))
     :stream-handler (StreamHandler. (:streams-directories stream)
                                     (:io-directories stream)
                                     (Object.)
                                     {}
                                     {}
                                     {}
                                     {}
                                     {}
                                     memtable-engine
                                     queue-component
                                     registry)
     :handler (-> (http/map->ChainHandler {})
                  (component/using [:api-handler :registry]))
     :api-handler (-> (handler/map->Handler {:memtable-engine memtable-engine})
                      (component/using [:stream-handler :registry]))
     :shared-event-executor (transport/event-executor)
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
      (alter-var-root #'system (constantly sys))
      (reload!))
    (catch Exception e
      (log/error {} e "fail to start the system")
      (throw e))))

(defn -main
  "Starts the application"
  [& args]
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
