(ns mirabelle.output.file
  (:require [cheshire.core :as json]
            [mirabelle.event :as e]
            [mirabelle.io :as io]
            [mirabelle.time :as time])
  (:import java.time.format.DateTimeFormatter
           java.time.Instant
           java.time.ZoneOffset))

(defn format-path
  [path fields-suffix date-suffix]
  (let [fields-suffix-builder (fn [event]
                                (reduce #(conj %1
                                               (if (sequential? %2)
                                                 (get-in event %2)
                                                 (get event %2)))
                                        []
                                        fields-suffix))
        formatter (when date-suffix
                    (DateTimeFormatter/ofPattern date-suffix))
        fields-suffix-fn (fn [p event] (apply format p (fields-suffix-builder event)))
        date-suffix-fn (fn [p event]
                         (let [time (.atZone (Instant/ofEpochMilli (* 1000 (:time event (time/now))))
                                             ZoneOffset/UTC)]
                           (format "%s-%s" p (.format formatter time))))]
    (fn [event]
      (let [path (if fields-suffix
                   (fields-suffix-fn path event)
                   path)]
        (if date-suffix
          (date-suffix-fn path event)
          path)))))

(def json-fn #(str (json/generate-string %) "\n"))

(defn gen-format-fn
  [format]
  (condp = format
    :json json-fn
    json-fn))

(defrecord File [format-path-fn format-fn]
  io/Output
  (inject! [_ events]
    (doseq [event (e/sequential-events events)]
      (spit (format-path-fn event) (format-fn event) :append true))))

(defn file-output
  [{:keys [path fields-suffix date-suffix format]}]
  (File. (format-path path fields-suffix date-suffix) (gen-format-fn format)))
