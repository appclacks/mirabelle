(ns mirabelle.output.file
  (:require [mirabelle.io :as io]
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
                           (format "%s-%s" p (.format formatter time))))
        ]
    (fn [event]
      (let [path (if fields-suffix
                   (fields-suffix-fn path event)
                   path)]
        (if date-suffix
          (date-suffix-fn path event)
          path)))))

(defrecord File [format-path-fn]
  io/Output
  (inject! [_ events]
    (doseq [event events]
      (spit (format-path-fn event) (str (pr-str event) "\n") :append true))))

(defn file-output
  [{:keys [path fields-suffix date-suffix]}]
  (File. (format-path path fields-suffix date-suffix)))
