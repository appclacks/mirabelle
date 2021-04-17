(streams
 (stream
  {:name :foo}
  (where [:> :metric 10]
         (increment
          (push-io! :influxdb)
          (info)))))
