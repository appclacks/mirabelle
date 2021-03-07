(ns mirabelle.config
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [environ.core :as env]
            [exoscale.cloak :as cloak]
            [exoscale.ex :as ex]))

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

(s/def ::tcp (s/keys :req-un [::host
                              ::port]
                     :opt-un [::cacert
                              ::cert
                              ::key]))

(s/def ::streams-directories (s/coll-of ::directory-spec))
(s/def ::io-directories (s/coll-of ::directory-spec))
(s/def ::actions (s/map-of keyword? symbol?))

(s/def ::stream (s/keys :opt-un [::streams-directories
                                 ::io-directories
                                 ::actions]))
(s/def ::directory ::directory-spec)
(s/def ::queue (s/keys :req-un [::directory]))
(s/def ::config (s/keys :req-un [::tcp ::queue]
                        :opt-un [::stream]))

(defmethod aero/reader 'secret
  [_ _ value]
  (cloak/mask value))

(defn load-config
  []
  (let [config (aero/read-config (env/env :mirabelle-configuration) {})]
    (if (s/valid? ::config config)
      config
      (throw (ex/ex-info
              "Invalid configuration"
              [::invalid [:corbi/user ::ex/incorrect]])))))
