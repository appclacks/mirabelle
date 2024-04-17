(ns mirabelle.prometheus
  (:import prometheus.Remote$WriteRequest
           prometheus.Types$TimeSeries
           prometheus.Types$TimeSeries$Builder
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
  "Converts a Prometheus WriteRequest into Mirabelle events"
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


(defn events->write-request
  "Converts list of events to a Prometheus remote write request"
  [events labels]
  (let [write-request (Remote$WriteRequest/newBuilder)
        timeseries (atom {})]
    (doseq [event events]
      (let [labels (-> (select-keys event labels)
                       (assoc "__name__" (:service event)))
            ^prometheus.Types$TimeSeries$Builder timeserie
            (get
             @timeseries
             labels
             (let [timeserie (Types$TimeSeries/newBuilder)]
               (doseq [[k v] labels]
                 (.addLabels timeserie
                             (doto (Types$Label/newBuilder)
                               (.setName (name k))
                               (.setValue (name v))
                               (.build))))
               (swap! timeseries assoc labels timeserie)
               timeserie))]
        (println @timeseries)
        (.addSamples timeserie
                     (doto (Types$Sample/newBuilder)
                       (.setValue (:metric event 0))
                       (.setTimestamp (int (* 1000 (:time event))))
                       (.build)))))
    (doseq [[_ ^prometheus.Types$TimeSeries$Builder timeserie] @timeseries]
      (.addTimeseries write-request (.build timeserie)))
    (.build write-request)))
