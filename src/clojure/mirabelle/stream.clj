(ns mirabelle.stream
  (:require [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.set :as set]
            [clojure.string :as string]
            [corbihttp.log :as log]
            [com.stuartsierra.component :as component]
            [exoscale.ex :as ex]
            [mirabelle.action :as action]
            [mirabelle.io.file :as io-file]))

(defn compile!
  [context stream]
  (when (seq stream)
    (for [s stream]
      (let [children (compile! context (:children s))
            func (get action/action->fn (:action s))
            params (:params s)]
        (if (seq params)
          (apply func context (concat params children))
          (apply func context children))))))

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

(defn new-stream-config
  [old-config new-config]
  (let [old-streams-names (streams-names old-config)
        new-streams-names (streams-names new-config)
        streams-to-remove (set/difference old-streams-names new-streams-names)
        streams-to-add (set/difference  new-streams-names old-streams-names)
        streams-to-compare (set/union old-streams-names new-streams-names)
        streams-to-reload (remove
                           (fn [stream-name]
                             (= (dissoc (get old-config stream-name) :entrypoint)
                                (dissoc (get new-config stream-name) :entrypoint)))
                           streams-to-compare)]

    {:to-remove streams-to-remove
     :to-add streams-to-add
     :to-reload streams-to-reload}))

(defn new-io-config
  [old-config new-config]
  (let [old-io-names (io-names old-config)
        new-io-names (io-names new-config)
        io-to-remove (set/difference old-io-names new-io-names)
        io-to-add (set/difference  new-io-names old-io-names)
        io-to-compare (set/union old-io-names new-io-names)
        io-to-reload (remove
                           (fn [stream-name]
                             (= (dissoc (get old-config stream-name) :entrypoint)
                                (dissoc (get new-config stream-name) :entrypoint)))
                           io-to-compare)]
    {:to-remove io-to-remove
     :to-add io-to-add
     :to-reload io-to-reload}))

(defprotocol IStreamHandler
  (reload [this] "Add the new configuration")
  (add-dynamic-stream [this stream-configuration] "Add a new stream")
  (remove-dynamic-stream [this stream-name] "Remove a stream by name")
  (push! [this event streams] "Inject an event into a list of streams"))

(defn debug
  [f]
  (println f)
  f)

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
                        lock ;; runtime
                        ^:volatile-mutable streams-configurations ;; runtime, the streams config
                        ^:volatile-mutable io-configurations;; runtime, the io config
                        ^:volatile-mutable compiled-real-time-streams
                        ^:volatile-mutable compiled-dynamic-streams
                        ^:volatile-mutable compiled-io
                        memtable-engine
                        memtable-executor
                        ]
  component/Lifecycle
  (start [this]
    this)
  (stop [this]
    (doseq [[_ io] compiled-io]
      (component/stop io)))
  IStreamHandler
  (reload [this]
    (locking lock
      (log/info {} "Reloading streams")
      (let [new-streams-configurations (read-edn-dirs streams-directories)
            new-io-configurations (read-edn-dirs io-directories)
            io-config (new-io-config io-configurations new-io-configurations)
            io-configs-to-compile (select-keys new-io-config
                                               (set/union (:to-add io-config)
                                                          (:to-reload io-config)))
            new-compiled-io (->> io-configs-to-compile
                                 (map (fn [[k v]] [k (compile-io! v)]))
                                 (into {}))
            io-final (-> (apply dissoc compiled-io (:to-remove io-config))
                         (merge new-compiled-io))
            ;; streams part
            {:keys [to-remove to-add to-reload]}
            (new-stream-config streams-configurations new-streams-configurations)
            ;; new or streams to reload should be added to the current config
            ;; should be compiled first
            ;; todo filter real time streams
            streams-configs-to-compile (select-keys new-streams-configurations
                                                   (set/union to-add to-reload))
            new-compiled-streams (->> streams-configs-to-compile
                                      ;; new io are injected into streams
                                      (map (fn [[k v]] [k (compile-stream!
                                                           {:memtable-engine memtable-engine
                                                            :memtable-executor memtable-executor
                                                            :io io-final} v)]))
                                      (into {}))
            streams-final (-> (apply dissoc compiled-real-time-streams to-remove)
                              (merge new-compiled-streams))]
        ;; all io not used anymore should be stopped
        (doseq [io-to-stop (set/union (:to-reload io-config)
                                      (:to-remove io-config))]
          (log/infof {} "Stopping IO %s" io-to-stop)
          (component/stop (-> (get compiled-io io-to-stop) :component)))
        (when (seq to-remove)
          (log/infof {} "Removing streams %s" (string/join #", " to-remove)))
        (when (seq to-reload)
          (log/infof {} "Reloading streams %s" (string/join #", " to-reload)))
        (when (seq to-add)
          (log/infof {} "Loading new streams %s" (string/join #", " to-add)))

        ;; mutate what is needed
        (set! streams-configurations new-streams-configurations)
        (set! io-configurations new-io-configurations)
        (set! compiled-real-time-streams streams-final)
        (set! compiled-io io-final))))
  (push! [this event stream]
    (if (= :streaming stream)
      (doseq [[_ s] compiled-real-time-streams]
        (stream! s event))
      (if-let [[_ s] (get compiled-dynamic-streams stream)]
        (stream! s event)
        (throw (ex/ex-incorrect (format "Stream %s not found" stream)))))))

(comment
  (defrecord StreamHandler [streams-directories ;; config
                                  io-directories;; config
                                  config-streams ;; runtime, the streams from the config
                                  dynamic-streams ;; runtime, the dynamic streams
                                  io-map ;; The IO
                                  memtable-engine ;; dependency
                                  memtable-executor ;; dependency
                                  ]
          component/Lifecycle
          (start [this]
            (assoc this
                   :config-streams {}
                   :dynamic-streams {}
                   :io-map {}))
          (stop [this]
            (doseq [[_ io] io-map]
              (component/stop io)))
          IStreamHandler
          (reload [this]
            (log/info {} "Reloading streams")
            (let [new-streams-configurations (read-edn-dirs streams-directories)
                  new-io-configurations (read-edn-dirs io-directories)
                  io-config (new-io-config io-map new-io-configurations)
                  io-to-merge (->> (select-keys new-io-config
                                                (set/union (:to-add io-config)
                                                           (:to-reload io-config)))
                                   (map (fn [[k v]] [k (compile-io! v)]))
                                   (into {}))
                  new-io (-> (apply dissoc io-map (:to-remove io-config))
                             (merge io-to-merge))
                  ;; streams part
                  {:keys [to-remove to-add to-reload]}
                  (new-stream-config config-streams new-streams-configurations)
                  ;; new or streams to reload should be added to the current config
                  ;; should be compiled first
                  streams-to-merge (->> (select-keys new-streams-configurations
                                                     (set/union to-add to-reload))
                                        ;; new io are injected into streams
                                        (map (fn [[k v]] [k (compile-stream!
                                                             {:memtable-engine memtable-engine
                                                              :memtable-executor memtable-executor
                                                              :io new-io} v)]))
                                        (into {}))
                  new-streams (-> (apply dissoc config-streams to-remove)
                                  (merge streams-to-merge))]
              ;; all io not used anymore should be stopped
              (doseq [io-to-stop (set/union (:to-reload io-config)
                                            (:to-remove io-config))]
                (log/infof {} "Stopping IO %s" io-to-stop)
                (component/stop (-> (get io-map io-to-stop) :component)))
              (when (seq to-remove)
                (log/infof {} "Removing streams %s" (string/join #", " to-remove)))
              (when (seq to-reload)
                (log/infof {} "Loading streams %s" (string/join #", " to-add)))
              (assoc this
                     :config-streams new-streams
                     :io-map new-io)))
          (push! [this event stream]
            (if (= :streaming stream)
              (doseq [[_ s] config-streams]
                (stream! s event))
              (if-let [[_ s] (get dynamic-streams stream)]
                (stream! s event)
                (throw (ex/ex-incorrect (format "Stream %s not found" stream))))))))
