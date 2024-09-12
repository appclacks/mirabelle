(ns mirabelle.prometheus
  (:import prometheus.Remote$WriteRequest
           prometheus.Types$TimeSeries
           prometheus.Types$TimeSeries$Builder
           prometheus.Types$Label
           prometheus.Types$Sample
           ))

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
                             (.getLabelsList series)))
          events (map
                  (fn [^Types$Sample sample]
                    (assoc {:attributes (dissoc labels :__name__)}
                           :name (:__name__ labels)
                           :metric (.getValue sample)
                           ;; prometheus time is in ms
                           :time (* (.getTimestamp sample) 1000000)))
                  (.getSamplesList series))]
      events)))


(defn events->write-request
  "Converts list of events to a Prometheus remote write request"
  [events]
  (let [write-request (Remote$WriteRequest/newBuilder)
        timeseries (loop [events events
                          timeseries {}]
                     (if-let [event (first events)]
                       (let [labels (cond-> (:attributes event)
                                      (:state event) (assoc :state (:state event))
                                      (:service event) (assoc :service (:service event))
                                      (get-in event [:attributes :name]) (assoc :__name__ (get-in event [:attributes :name]))
                                      (:name event) (assoc :__name__ (:name event))
                                      true (dissoc :name)
                                      )
                             ^prometheus.Types$TimeSeries$Builder timeserie
                             (get
                              timeseries
                              labels
                              (let [timeserie (Types$TimeSeries/newBuilder)]
                                (doseq [[k v] labels]
                                  (.addLabels timeserie
                                              (doto (Types$Label/newBuilder)
                                                (.setName (name k))
                                                (.setValue (name v))
                                                (.build))))
                                timeserie))
                             sample (doto (Types$Sample/newBuilder)
                                      (.setValue (:metric event))
                                      (.setTimestamp (long (/ (:time event) 1000000)))
                                      )
                             ]
                         (.addSamples timeserie ^Types$Sample (.build sample))
                         (recur (next events) (assoc timeseries labels timeserie)))
                       timeseries))]
    (doseq [[_ ^prometheus.Types$TimeSeries$Builder timeserie] timeseries]
      (.addTimeseries write-request (.build timeserie)))
    (.build write-request)))

