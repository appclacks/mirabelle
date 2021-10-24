(ns mirabelle.spec
  (:require [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [corbihttp.log :as log]
            [exoscale.cloak :as cloak]
            [exoscale.ex :as ex]))

(s/def ::ne-string (s/and string? (complement string/blank?)))
(s/def ::secret cloak/secret?)
(s/def ::keyword-or-str (s/or :keyword keyword?
                              :string ::ne-string))

(defn not-null [v] (not (nil? v)))

(defn valid?
  [spec value]
  (ex/assert-spec-valid spec value))

(defn valid-action?
  [spec value]
  (try
    (valid? spec value)
    (catch Exception e
      (log/error {} (format "Invalid call to action '%s' with parameters '%s'"
                            (name spec)
                            (pr-str value)))
      (throw e))))

(s/def :mirabelle.index/query ::ne-string)
(s/def :mirabelle.stream/name keyword?)
(s/def :mirabelle.stream/config ::ne-string)
(s/def :mirabelle.stream/event map?)

(s/def :mirabelle.http.index/search (s/keys :req-un [:mirabelle.index/query
                                                     :mirabelle.stream/name]))

(s/def :mirabelle.http.index/current-time (s/keys :req-un [:mirabelle.stream/name]))

(s/def :mirabelle.http.stream/add (s/keys :req-un [:mirabelle.stream/name
                                                   :mirabelle.stream/config]))

(s/def :mirabelle.http.stream/get (s/keys :req-un [:mirabelle.stream/name]))
(s/def :mirabelle.http.stream/event (s/keys :req-un [:mirabelle.stream/name
                                                     :mirabelle.stream/event]))
(s/def :mirabelle.http.stream/remove (s/keys :req-un [:mirabelle.stream/name]))
