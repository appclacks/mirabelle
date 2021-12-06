(ns mirabelle.config
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [clojure.pprint :as pprint]
            [clojure.spec.alpha :as s]
            [corbihttp.spec :as spec]
            [corbihttp.error :as error]
            [corbihttp.http :as http]
            [corbihttp.log :as log]
            [environ.core :as env]
            [exoscale.cloak :as cloak]
            [exoscale.ex :as ex]
            ;; needed to eval the config
            [mirabelle.action :refer :all]
            [mirabelle.path :as path])
  (:import java.io.File))

(s/def ::non-empty-string (s/and string? not-empty))

(s/def ::native? boolean)
(s/def ::event-executor-size pos-int?)

(s/def ::tcp (s/keys :req-un [::spec/host
                              ::spec/port]
                     :opt-un [::spec/cacert
                              ::spec/cert
                              ::spec/key
                              ::native?
                              ::event-executor-size]))

(s/def ::websocket (s/keys :req-un [::spec/host
                                    ::spec/port]))

(s/def ::directories (s/coll-of ::spec/directory-spec))

(s/def ::actions (s/map-of keyword? symbol?))
(s/def ::stream (s/keys :opt-un [::directories
                                 ::io-directories
                                 ::actions]))

(s/def ::io (s/keys :opt-un [::directories]))
(s/def ::test (s/keys :opt-un [::directories]))
(s/def ::roll-cycle keyword?)

(s/def ::directory ::spec/directory-spec)
(s/def ::queue (s/keys :req-un [::directory]
                       :opt-un [::roll-cycle]))
(s/def ::config (s/keys :req-un [::tcp ::websocket ::http/http]
                        :opt-un [::stream ::io ::test ::queue]))

(defmethod aero/reader 'secret
  [_ _ value]
  (cloak/mask value))

(defn load-config
  []
  (let [config-path (env/env :mirabelle-configuration)
        _ (when-not config-path
            (throw (ex/ex-info
                     "The MIRABELLE_CONFIGURATION variable should contain the path to the main configuration file."
                    [::invalid [:corbi/user ::ex/incorrect]])))
        config (aero/read-config config-path {})]
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
  (let [profile (get-env-profile)]
    [(.getName f)
     (eval (aero/read-config (.getPath f)
                             (cond-> {}
                               profile (assoc :profile profile))))]))

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
        (throw (ex/ex-incorrect (str (error/spec-ex->message e) "\nMore details about the error:\n")
                                {:original-error e}))
        (throw e)))))
