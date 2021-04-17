(ns mirabelle.spec
  (:require [clojure.spec.alpha :as s]
            [exoscale.ex :as ex]))

(s/def ::ne-string (s/and string? (complement empty?)))
(s/def ::keyword-or-str (s/or :keyword keyword?
                              :string ::ne-string))

(defn not-null [v] (not (nil? v)))

(defn valid?
  [spec value]
  (ex/assert-spec-valid spec value))
