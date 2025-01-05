(ns mirabelle.spec
  (:require [clojure.spec.alpha :as s]
            [corbihttp.log :as log]
            [corbihttp.spec :as spec]
            [exoscale.ex :as ex]))

(s/def ::keyword-or-str (s/or :keyword keyword?
                              :string ::spec/ne-string))

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

(s/def :mirabelle.stream/name keyword?)
(s/def :mirabelle.stream/config ::spec/ne-string)
(s/def :mirabelle.stream/event map?)
(s/def :mirabelle.stream/events (s/coll-of :mirabelle.stream/event))

(s/def :mirabelle.http.index/current-time (s/keys :req-un [:mirabelle.stream/name]))

(s/def :mirabelle.http.stream/add (s/keys :req-un [:mirabelle.stream/name
                                                   :mirabelle.stream/config]))

(s/def :mirabelle.http.stream/get (s/keys :req-un [:mirabelle.stream/name]))
(s/def :mirabelle.http.stream/events (s/keys :req-un [:mirabelle.stream/name
                                                      :mirabelle.stream/events]))
(s/def :mirabelle.http.fluentbit/post (s/keys :req-un [:mirabelle.stream/name]))
(s/def :mirabelle.http.prometheus/remote-write (s/keys :req-un [:mirabelle.stream/name]))
(s/def :mirabelle.http.stream/remove (s/keys :req-un [:mirabelle.stream/name]))
