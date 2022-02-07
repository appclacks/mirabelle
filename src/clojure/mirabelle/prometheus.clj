(ns mirabelle.prometheus
  (:import prometheus.Remote$WriteRequest
           prometheus.Types$TimeSeries
           prometheus.Types$Label
           prometheus.Types$Sample))

(defn name->service
  "Renames the prometheus __name__ label to :service"
  [labels]
  (-> (assoc labels :service (:__name__ labels))
      (dissoc :__name__)))

;; todo metric metadata iteration, make it optional ?
;; optional = all metadata removed or remove only help ?
(defn write-request->events
  "Converts a Prometheus Prometheus WriteRequest into Mirabelle events"
  [^Remote$WriteRequest write-request]
  (for [^Types$TimeSeries series (.getTimeseriesList write-request)]
    (let [labels (-> (reduce (fn [state ^Types$Label label]
                               (assoc state
                                      (keyword (.getName label))
                                      (.getValue label)))
                             {}
                             (.getLabelsList series))
                     name->service)
          events (map
                  (fn [^Types$Sample sample]
                    (assoc labels
                           :metric (.getValue sample)
                           ;; prometheus time is in ms
                           :time (double (/ (.getTimestamp sample) 1000))))
                  (.getSamplesList series))]
      events)))
