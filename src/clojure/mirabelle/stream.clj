(ns mirabelle.stream
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as string]
            [corbihttp.log :as log]
            [corbihttp.metric :as metric]
            [com.stuartsierra.component :as component]
            [exoscale.ex :as ex]
            [mirabelle.action :as action]
            [mirabelle.db.queue :as q]
            [mirabelle.index :as index]
            [mirabelle.io.file :as io-file]
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
               (if (seq params)
                 (apply func context (concat params children))
                 (apply func context children))))
           (ex/ex-incorrect! (format "Action %s not found" action))))))))

(defn compile-stream!
  "Compile a stream to functions and associate to it its entrypoint."
  [context stream]
  (assoc stream
         :context context
         :entrypoint
         (->> [(:actions stream)]
              (compile! context)
              first)))

(defn compile-io!
  "Config an IO configuration.
  {:type :file :config {:path ...}}.

  Adds the :component key to the IO"
  [io-config custom-io]
  (let [t (:type io-config)]
    (cond
      ;; it's a custom IO
      ;; need to resolve the fn from the config
      (get custom-io t)
      (assoc io-config
             :component
             ((requiring-resolve (get custom-io t)) (:config io-config)))

      (= :async-queue t)
      (assoc io-config :component (pool/dynamic-thread-pool-executor (:config io-config)))

      (= :file t)
      (assoc io-config
             :component
             (io-file/map->FileIO (:config io-config)))
      :else
      (throw (ex/ex-incorrect
              (format "Invalid IO: %s" t)
              io-config)))))

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
  (context [this] "Return the streams context")
  (reload [this] "Add the new configuration")
  (add-dynamic-stream [this stream-name stream-configuration] "Add a new stream")
  (remove-dynamic-stream [this stream-name] "Remove a stream by name")
  (list-dynamic-streams [this] "List dynamic streams")
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
       (map slurp)
       (map edn/read-string)
       (apply merge)))

;; I should simplify this crappy code
(deftype StreamHandler [streams-directories ;; config
                        io-directories;; config
                        lock
                        ^:volatile-mutable streams-configurations
                        ^:volatile-mutable io-configurations
                        custom-actions
                        custom-io
                        ^:volatile-mutable compiled-real-time-streams
                        ^:volatile-mutable compiled-dynamic-streams
                        ^:volatile-mutable compiled-io
                        ^:volatile-mutable ^Timer stream-timer
                        queue
                        registry
                        tap
                        test-mode?
                        index
                        ]
  component/Lifecycle
  (start [this]
    (let [new-io-configurations (read-edn-dirs io-directories)
          new-compiled-io (->> new-io-configurations
                               (map (fn [[k v]] [k (compile-io! v custom-io)]))
                               (into {}))
          timer (metric/get-timer! registry
                                   :stream-duration
                                   {})]
      (when-let [io (seq (map first new-io-configurations))]
        (log/infof {}
                   "Adding IO %s"
                   (string/join #", " io)))
      (set! io-configurations new-io-configurations)
      (set! compiled-io new-compiled-io)
      (set! compiled-dynamic-streams {})
      (set! stream-timer timer)
      (reload this)
      ;; reload events from the queue
      (when queue
        (q/read-all! queue #(push! this
                                   (update %1 :tags concat ["discard"])
                                   :streaming))))
    this)
  (stop [this]
    ;; stop executors first to let them finish ongoing tasks
    (doseq [[_ queue] (filter #(= :async-queue (:type %)) compiled-io)]
      (let [^Executor executor (:component queue)]
        (pool/shutdown executor)))
    (doseq [[_ io] (remove #(= :async-queue (:type %)) compiled-io)]
      (component/stop io)))
  IStreamHandler
  (context [this]
    {:io compiled-io
     :index index
     :queue queue
     :tap tap
     :test-mode? test-mode?
     :custom-actions custom-actions
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
                                      ;; new io are injected into streams
                                      (mapv (fn [[k v]]
                                              [k (compile-stream!
                                                  (context this)
                                                  v)]))
                                      (into {})
                                      (merge (apply dissoc
                                                    compiled-real-time-streams to-remove)))]
        (when (seq to-remove)
          (log/infof {} "Removing streams %s" (string/join #", " to-remove)))
        (when (seq to-reload)
          (log/infof {} "Reloading streams %s" (string/join #", " to-reload)))
        (when (seq to-add)
          (log/infof {} "Adding new streams %s" (string/join #", " to-add)))

        ;; mutate what is needed
        (set! compiled-real-time-streams new-compiled-streams)
        (set! streams-configurations new-streams-configurations)
        {:compiled-real-time-streams compiled-real-time-streams
         :streams-configurations new-streams-configurations})))
  (push! [this event stream]
    (if (= :streaming stream)
      (doseq [[_ s] compiled-real-time-streams]
        (let [t1 (System/nanoTime)]
          (stream! s event)
          (.record stream-timer (- (System/nanoTime) t1) TimeUnit/NANOSECONDS)))
      (if-let [s (get compiled-dynamic-streams stream)]
        (stream! s event)
        (throw (ex/ex-incorrect (format "Stream %s not found" stream))))))
  (add-dynamic-stream [this stream-name stream-configuration]
    (locking lock
      (log/infof {} "Adding dynamic stream %s" stream-name)
      (let [compiled-stream (compile-stream!
                             ;; dedicated index for dyn streams
                             (assoc (context this)
                                    :index
                                    (index/map->Index {}))
                             stream-configuration)
            new-compiled-dynamic-streams (assoc compiled-dynamic-streams
                                                stream-name
                                                compiled-stream)]
        (set! compiled-dynamic-streams new-compiled-dynamic-streams))))
  (remove-dynamic-stream [this stream-name]
    (log/infof {} "Removing dynamic stream %s" stream-name)
    (locking lock
      (let [new-compiled-dynamic-streams (dissoc compiled-dynamic-streams
                                                 stream-name)]
        (set! compiled-dynamic-streams new-compiled-dynamic-streams))))
  (list-dynamic-streams [this]
    (keys compiled-dynamic-streams)))

(defn map->StreamHandler
  [{:keys [streams-directories
           io-directories
           streams-configurations
           io-configurations
           custom-actions
           custom-io
           compiled-real-time-streams
           compiled-dynamic-streams
           compiled-io
           stream-timer
           queue
           registry
           test-mode?
           index]
    :or {streams-configurations {}
         io-configurations {}
         custom-actions {}
         custom-io {}
         compiled-real-time-streams {}
         compiled-dynamic-streams {}
         compiled-io {}
         test-mode? false}}]
  (->StreamHandler streams-directories
                   io-directories
                   (Object.)
                   streams-configurations
                   io-configurations
                   custom-actions
                   custom-io
                   compiled-real-time-streams
                   compiled-dynamic-streams
                   compiled-io
                   stream-timer
                   queue
                   registry
                   (atom {})
                   test-mode?
                   index))
