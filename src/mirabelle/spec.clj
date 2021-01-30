(ns mirabelle.spec
  (:require [clojure.spec.alpha :as s]
            [exoscale.ex :as ex]))

(defn not-null [v] (not (nil? v)))

(defn valid?
  [spec value]
  (ex/assert-spec-valid spec value))
