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
            [mirabelle.io.file :as io-file]))

(defn compile!
  [context stream]
  (when (seq stream)
    (doall
     (for [s stream]
       (let [action (:action s)
             func (get action/action->fn action)
             params (:params s)]
         ;; pass an fn compiling children to by
         ;; in order to generate one children per fork
         (if (= :by action)
           (action/by-fn (first params)
                         #(compile! context (:children s)))
           (let [children (compile! context (:children s))]
             (if (seq params)
               (apply func context (concat params children))
               (apply func context children)))))))))

(defn compile-stream!
  "Compile a stream to functions and associate to it its entrypoint."
  [context stream]
  (assoc stream :entrypoint
         (->> [(:actions stream)]
              (compile! context)
              first)))

(defn compile-io!
  "Config an IO configuration.
  {:type :file :config {:path ...}}.

  Adds the :component key to the IO"
  [io-config]
  (condp = (:type io-config)
    :file (assoc io-config
                 :component
                 (io-file/map->FileIO (:config io-config)))
    (throw (ex/ex-incorrect
            (format "Invalid IO: %s" (:type io-config))
            io-config))))

(defn stream!
  [stream event]
  ((:entrypoint stream) event))

(defn streams-names
  "Returns, from a configuration, the streams names as a set"
  [streams-config]
  (->> streams-config
       keys
       (map keyword)
       set))

(def io-names streams-names)

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
        to-add (set/difference  new-names old-names)
        to-compare (set/union old-names new-names)
        to-reload (remove
                   (fn [n]
                     (= (get old-config n)
                        (get new-config n)))
                   to-compare)]

    {:to-remove to-remove
     :to-add to-add
     :to-reload to-reload}))

(defprotocol IStreamHandler
  (reload [this] "Add the new configuration")
  (add-dynamic-stream [this stream-name stream-configuration] "Add a new stream")
  (remove-dynamic-stream [this stream-name] "Remove a stream by name")
  (list-dynamic-streams [this] "List dynamic streams")
  (push! [this event streams] "Inject an event into a list of streams"))

(defn read-edn-dirs
  [dirs-path]
  (->> (map (fn [path] (.listFiles (io/file path))) dirs-path)
       (map (fn [files]
              (for [f files]
                (.getPath f))))
       (reduce #(concat %2 %1) [])
       (map slurp)
       (map edn/read-string)
       (apply merge)))

(deftype StreamHandler [streams-directories ;; config
                        io-directories;; config
                        lock
                        ^:volatile-mutable streams-configurations ;; runtime, the streams config
                        ^:volatile-mutable io-configurations;; runtime, the io config
                        ^:volatile-mutable compiled-real-time-streams
                        ^:volatile-mutable compiled-dynamic-streams
                        ^:volatile-mutable compiled-io
                        memtable-engine
                        queue
                        registry
                        ]
  component/Lifecycle
  (start [this]
    (let [new-io-configurations (read-edn-dirs io-directories)
          new-compiled-io (->> new-io-configurations
                               (map (fn [[k v]] [k (compile-io! v)]))
                               (into {}))]
      (when-let [io (seq (map first new-io-configurations))]
        (log/infof {}
                   "Adding IO %s"
                   (string/join #", " io)))
      (set! io-configurations new-io-configurations)
      (set! compiled-io new-compiled-io)
      (set! compiled-dynamic-streams {}))
    this)
  (stop [this]
    (doseq [[_ io] compiled-io]
      (component/stop io)))
  IStreamHandler
  (reload [this]
    ;; I should simplify this crappy code
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
                                                  {:memtable-engine memtable-engine
                                                   :io compiled-io
                                                   :queue queue
                                                   :reinject #(push! this %1 %2)} v)]))
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
        (set! streams-configurations new-streams-configurations))))
  (push! [this event stream]
    (if (= :streaming stream)
      (doseq [[_ s] compiled-real-time-streams]
        (stream! s event))
      (if-let [s (get compiled-dynamic-streams stream)]
        (stream! s event)
        (throw (ex/ex-incorrect (format "Stream %s not found" stream))))))
  (add-dynamic-stream [this stream-name stream-configuration]
    (locking lock
      (log/infof {} "Adding dynamic stream %s" stream-name)
      (let [compiled-stream (compile-stream!
                             {:memtable-engine memtable-engine
                              :io compiled-io
                              :queue queue}
                             stream-configuration)
            new-compiled-dynamic-streams (assoc compiled-dynamic-streams
                                                stream-name
                                                compiled-stream)]
        (set! compiled-dynamic-streams new-compiled-dynamic-streams))))
  (remove-dynamic-stream [this stream-name]
    (log/infof {} "Removing dynamic stream %s" stream-name)
    (locking lock
      (let [new-compiled-dynamic-streams (assoc compiled-dynamic-streams
                                                stream-name)]
        (set! compiled-dynamic-streams new-compiled-dynamic-streams))))
  (list-dynamic-streams [this]
    (keys compiled-dynamic-streams)))
