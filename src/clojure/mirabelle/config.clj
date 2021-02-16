(ns mirabelle.config
  (:require [aero.core :as aero]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [environ.core :as env]
            [exoscale.cloak :as cloak]
            [exoscale.ex :as ex]))

(s/def ::file (fn [path]
                (let [file (io/file path)]
                  (and (.exists file)
                       (.isFile file)))))
(s/def ::directory (fn [path]
                     (let [file (io/file path)]
                       (and (.exists file)
                            (.isDirectory file)))))
(s/def ::non-empty-string (s/and string? not-empty))

(s/def ::host ::non-empty-string)
(s/def ::port pos-int?)
(s/def ::cacert ::file)
(s/def ::cert ::file)
(s/def ::key ::file)

(s/def ::tcp (s/keys :req-un [::host
                              ::port]
                     :opt-un [::cacert
                              ::cert
                              ::key]))

(s/def ::streams-directories (s/coll-of ::directory))
(s/def ::io-directories (s/coll-of ::directory))
(s/def ::stream (s/keys :opt-un [::streams-directories
                                 ::io-directories]))
(s/def ::config (s/keys :req-un [::tcp]
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
