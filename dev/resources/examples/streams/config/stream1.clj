(streams
 (stream
  {:name :foo}
  (where [:> :metric 10]
         (increment
          (with {:influxdb/tags [:environment :state :host]
                 :influxdb/fields [:metric]}
                (info)
                (push-io! :influxdb))
          ))))
