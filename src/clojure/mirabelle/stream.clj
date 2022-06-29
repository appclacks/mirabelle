;; the code from this namespace is BAD
(ns mirabelle.stream
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as string]
            [corbihttp.log :as log]
            [corbihttp.metric :as metric]
            [com.stuartsierra.component :as component]
            [exoscale.ex :as ex]
            [mirabelle.action :as action]
            mirabelle.config
            [mirabelle.index :as index]
            [mirabelle.output.elasticsearch :as elasticsearch]
            [mirabelle.output.file :as io-file]
            [mirabelle.output.influxdb :as influxdb]
            [mirabelle.output.pagerduty :as pagerduty]
            [mirabelle.pool :as pool])
  (:import [io.micrometer.core.instrument Timer]
           [java.io File]
           [java.util.concurrent TimeUnit Executor]))

(defn compile!
  [context stream]
  (when (seq stream)
    (doall
     (for [s stream]
       (let [action (:action s)
             func-action (get (merge action/action->fn
                                     (:custom-actions context))
                              action)
             func (if (symbol? func-action)
                    (requiring-resolve func-action)
                    func-action)
             params (:params s)]
         ;; verify if the fn is found or if we are in the special
         ;; case of the by stream
         (if (or (= :by action)
                 func)
           (if (= :by action)
             ;; pass an fn compiling children to by
             ;; in order to generate one children per fork
             (action/by-fn (first params)
                           #(compile! context (:children s)))
             (let [children (compile! context (:children s))]
               (try
                 (if (seq params)
                   (apply func context (concat params children))
                   (apply func context children))
                 (catch Exception e
                   (log/error {} (format "Your EDN configuration is incorrect. Error in action '%s' with parameters '%s'"
                                         (name action)
                                         (pr-str params)))
                   (throw e)))))
           (let [error (format "Your EDN configuration is incorrect. Action %s not found." action)]
             (log/error {} error)
             (ex/ex-incorrect! error))))))))

(defn compile-stream!
  "Compile a stream to functions and associate to it its entrypoint."
  [context stream]
  (assoc stream
         :context context
         :entrypoint
         (->> [(:actions stream)]
              (compile! context)
              first)))

(defn compile-output!
  "Config an output configuration.

  Adds the :component key to the output"
  [registry output-name output-config]
  (let [t (:type output-config)]
    (cond
      ;; it's a custom output
      ;; need to resolve the fn from the config
      (= :custom t)
      (assoc output-config
             :component
             ((requiring-resolve (:builder output-config)) (assoc (:config output-config)
                                                                  :registry registry)))

      (= :async-queue t)
      (assoc output-config :component (pool/dynamic-thread-pool-executor registry
                                                                         output-name
                                                                         (:config output-config)))

      (= :file t)
      (assoc output-config
             :component
             (io-file/map->File (:config output-config)))

      (= :pagerduty t)
      (assoc output-config
             :component
             (component/start (pagerduty/map->Pagerduty (:config output-config))))

      (= :elasticsearch t)
      (assoc output-config
             :component
             (component/start (elasticsearch/map->Elasticsearch
                               {:config (:config output-config)})))

      (= :influxdb t)
      (assoc output-config
             :component
             (component/start (influxdb/map->Influx
                               {:config (:config output-config)})))
      :else
      (throw (ex/ex-incorrect
              (format "Invalid Output: %s" t)
              output-config)))))

(defn stream!
  [stream event]
  ((:entrypoint stream) event))

(defn config-keys
  "Returns, from a configuration, the keys as a set"
  [config]
  (->> config
       keys
       (map keyword)
       set))

(defn new-config
  [old-config new-config]
  (let [old-names (config-keys old-config)
        new-names (config-keys new-config)
        to-remove (set/difference old-names new-names)
        to-add (set/difference new-names old-names)
        to-compare (set/intersection old-names new-names)
        to-reload (set (remove
                        (fn [n]
                          (= (get old-config n)
                             (get new-config n)))
                        to-compare))]
    {:to-remove to-remove
     :to-add to-add
     :to-reload to-reload}))

(defprotocol IStreamHandler
  (context [this source-stream] "Return the streams context")
  (reload [this] "Add the new configuration")
  (add-stream [this stream-name stream-configuration] "Add a new stream")
  (remove-stream [this stream-name] "Remove a stream by name")
  (list-streams [this] "List streams")
  (get-stream [this stream-name] "Get a  stream")
  (push! [this event streams] "Inject an event into a list of streams"))

(defn read-edn-dirs
  "returns the edn content from a list of directories.
  All files in the directories are read."
  [dirs-path]
  (->> (map (fn [path] (.listFiles (io/file path))) dirs-path)
       (map (fn [files]
              (for [f files]
                (.getPath ^File f))))
       (reduce #(concat %2 %1) [])
       (map #(aero/read-config % {}))
       (apply merge)))

(defn persisted-stream-file-name
  "Returns the name of the file which will contain the stream"
  [stream-name]
  (str (name stream-name) "-api.edn"))

;; I should simplify this crappy code
(deftype StreamHandler [streams-directories ;; config
                        outputs-configurations;; config
                        lock
                        ;; streams from the config file
                        ^:volatile-mutable streams-configurations
                        custom-actions
                        custom-outputs
                        ^:volatile-mutable compiled-streams
                        ^:volatile-mutable compiled-outputs
                        ^:volatile-mutable ^Timer stream-timer
                        queue
                        registry
                        tap
                        test-mode?
                        index
                        pubsub
                        ]
  component/Lifecycle
  (start [this]
    (let [new-compiled-outputs (->> outputs-configurations
                                    (map (fn [[k v]] [k (compile-output! registry
                                                                         k
                                                                         v)]))
                                    (into {}))
          timer (metric/get-timer! registry
                                   :stream-duration
                                   {})]
      (when-let [output (seq (map first outputs-configurations))]
        (log/infof {}
                   "Adding outputs %s"
                   (string/join #", " output)))
      (set! compiled-outputs new-compiled-outputs)
      (set! compiled-streams {})
      (set! stream-timer timer)
      (reload this))
    this)
  (stop [_]
    ;; stop executors first to let them finish ongoing tasks
    (doseq [[_ queue] (filter #(= :async-queue (:type %)) compiled-outputs)]
      (let [^Executor executor (:component queue)]
        (pool/shutdown executor)))
    (doseq [[_ io] (remove #(= :async-queue (:type %)) compiled-outputs)]
      (component/stop io)))
  IStreamHandler
  (context [this source-stream]
    {:outputs compiled-outputs
     :registry registry
     :queue queue
     :tap tap
     :test-mode? test-mode?
     :source-stream source-stream
     :custom-actions custom-actions
     :pubsub pubsub
     :reinject #(push! this %1 %2)})
  (reload [this]
    (locking lock
      (log/info {} "Reloading streams")
      (let [new-streams-configurations (read-edn-dirs streams-directories)
            {:keys [to-remove to-add to-reload]}
            (new-config streams-configurations new-streams-configurations)
            ;; new or streams to reload should be added to the current config
            ;; should be compiled first
            ;; todo filter real time streams
            streams-configs-to-compile (select-keys new-streams-configurations
                                                    (set/union to-add to-reload))
            new-compiled-streams (->> streams-configs-to-compile
                                      (mapv (fn [[k v]]
                                              [k (compile-stream!
                                                  (assoc (context this k)
                                                         :index (component/start (index/map->Index {}))
                                                         :default (boolean (:default v)))
                                                  (update v :default boolean))]))
                                      (into {})
                                      (merge (apply dissoc
                                                    compiled-streams to-remove)))]
        (when (seq to-remove)
          (log/infof {} "Removing streams %s" (string/join #", " to-remove)))
        (when (seq to-reload)
          (log/infof {} "Reloading streams %s" (string/join #", " to-reload)))
        (when (seq to-add)
          (log/infof {} "Adding new streams %s" (string/join #", " to-add)))

        ;; mutate what is needed
        (set! compiled-streams new-compiled-streams)
        (set! streams-configurations new-streams-configurations)
        {:compiled-streams compiled-streams
         :streams-configurations new-streams-configurations})))
  (push! [_ event stream]
    (if (= :default stream)
      (doseq [[_ s] compiled-streams]
        (when (:default s)
          (let [t1 (System/nanoTime)]
            (stream! s event)
            (.record stream-timer (- (System/nanoTime) t1) TimeUnit/NANOSECONDS))))
      (if-let [s (get compiled-streams stream)]
        (let [t1 (System/nanoTime)]
          (stream! s event)
          (.record stream-timer (- (System/nanoTime) t1) TimeUnit/NANOSECONDS))
        (throw (ex/ex-info
                (format "Stream %s not found" stream)
                [::not-found [:corbi/user ::ex/not-found]])))))
  (add-stream [this stream-name {:keys [persist] :as stream-configuration}]
    (log/infof {} "Adding stream %s" stream-name)
    (if persist
      (if-let [stream-directory (first streams-directories)]
        (do (log/infof {} "The stream %s will be persisted" stream-name)
            (spit (str stream-directory "/" (persisted-stream-file-name stream-name))
                  (pr-str {stream-name stream-configuration}))
            (reload this))
        (throw (ex/ex-info
                "The stream cannot be saved. No directory configured for streams"
                [::incorrect [:corbi/user ::ex/incorrect]])))
      (locking lock
        (let [compiled-stream (compile-stream!
                               (assoc (context this stream-name)
                                      :index
                                      (component/start (index/map->Index {})))
                               (update stream-configuration :default boolean))
              new-compiled-streams (assoc compiled-streams
                                          stream-name
                                          compiled-stream)]
          (set! compiled-streams new-compiled-streams)))))
  (remove-stream [this stream-name]
    (log/infof {} "Removing stream %s" stream-name)
    (let [stream-directory (first streams-directories)
          stream-file (str stream-directory "/" (persisted-stream-file-name stream-name))]
      (if (.exists ^java.io.File (io/file stream-file))
        ;; the file was created using the API so we will delete the file then reload
        (do
          (log/infof {} "The stream %s will be removed from the filesystem" stream-name)
          (io/delete-file stream-file true)
          (reload this))
        (locking lock
          (let [new-compiled-streams (dissoc compiled-streams
                                             stream-name)]
            (set! compiled-streams new-compiled-streams))))))
  (list-streams [_]
    (or (keys compiled-streams) []))
  (get-stream [_ stream-name]
    (if-let [stream (get compiled-streams stream-name)]
      stream
      (throw (ex/ex-info
              (format "stream %s not found" stream-name)
              [::not-found [:corbi/user ::ex/not-found]])))))

(defn map->StreamHandler
  [{:keys [streams-directories
           outputs-configurations
           streams-configurations
           custom-actions
           custom-outputs
           compiled-streams
           compiled-outputs
           stream-timer
           queue
           registry
           test-mode?
           index
           pubsub]
    :or {streams-configurations {}
         outputs-configurations {}
         custom-actions {}
         custom-outputs {}
         compiled-streams {}
         compiled-outputs {}
         test-mode? false}}]
  (->StreamHandler streams-directories
                   outputs-configurations
                   (Object.)
                   streams-configurations
                   custom-actions
                   custom-outputs
                   compiled-streams
                   compiled-outputs
                   stream-timer
                   queue
                   registry
                   (atom {})
                   test-mode?
                   index
                   pubsub))
