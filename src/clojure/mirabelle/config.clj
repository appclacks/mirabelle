(ns mirabelle.config
  (:require [aero.core :as aero]
            [clojure.edn :as edn]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [clojure.spec.alpha :as s]
            [corbihttp.error :as error]
            [corbihttp.log :as log]
            [environ.core :as env]
            [exoscale.cloak :as cloak]
            [exoscale.ex :as ex]
            [mirabelle.action :refer :all]
            [mirabelle.path :as path])
  (:import java.io.File))

(s/def ::file-spec (fn [path]
                     (let [file (io/file path)]
                       (and (.exists file)
                            (.isFile file)))))
(s/def ::directory-spec (fn [path]
                          (let [file (io/file path)]
                            (and (.exists file)
                                 (.isDirectory file)))))
(s/def ::non-empty-string (s/and string? not-empty))

(s/def ::host ::non-empty-string)
(s/def ::port pos-int?)
(s/def ::cacert ::file-spec)
(s/def ::cert ::file-spec)
(s/def ::key ::file-spec)
(s/def ::event-executor-size pos-int?)

(s/def ::tcp (s/keys :req-un [::host
                              ::port]
                     :opt-un [::cacert
                              ::cert
                              ::event-executor-size
                              ::key]))

(s/def ::websocket (s/keys :req-un [::host
                                    ::port]))

(s/def ::directories (s/coll-of ::directory-spec))

(s/def ::actions (s/map-of keyword? symbol?))
(s/def ::stream (s/keys :opt-un [::directories
                                 ::io-directories
                                 ::actions]))

(s/def ::io (s/keys :opt-un [::directories]))
(s/def ::test (s/keys :opt-un [::directories]))
(s/def ::roll-cycle keyword?)

(s/def ::directory ::directory-spec)
(s/def ::queue (s/keys :req-un [::directory]
                       :opt-un [::roll-cycle]))
(s/def ::config (s/keys :req-un [::tcp ::websocket]
                        :opt-un [::stream ::io ::test ::queue]))

(defmethod aero/reader 'secret
  [_ _ value]
  (cloak/mask value))

(defn load-config
  []
  (let [config (aero/read-config (env/env :mirabelle-configuration) {})]
    (if (s/valid? ::config config)
      config
      (throw (ex/ex-info
              (format "Invalid configuration: %s"
                      (s/explain-str ::config config))
              [::invalid [:corbi/user ::ex/incorrect]])))))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn- config-file?
  "Is the given File a configuration file?"
  [^File file]
  (let [filename (.getName file)]
    (and (.isFile file)
         (or (.matches filename ".*\\.clj$")
             (.matches filename ".*\\.config$")))))

(defn name->compiled
  "Get a file, returns a vector where the first value is the file name and the
  second value the compiled version of it."
  [^File f]
  (log/info {} (format "Compiling %s" (.getName f)))
  [(.getName f)
   (->> (.getPath f)
        slurp
        edn/read-string
        eval)])

(defn compile-config!
  [src-dir dest-dir]
  (try
    (binding [*ns* (find-ns 'mirabelle.config)]
      (->> src-dir
           io/file
           file-seq
           (filter config-file?)
           (sort)
           (map name->compiled)
           (map #(pprint/pprint (second %)
                                (io/writer (path/new-path dest-dir (first %)))))
           dorun))
    (catch Exception e
      (if (ex/type? e ::ex/invalid-spec)
        (do (log/error {} e)
            (throw (ex/ex-incorrect (error/spec-ex->message e)
                                    {})))
        (throw e)))))
