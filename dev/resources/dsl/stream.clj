(streams
  (stream {:name :test}
          (where [:not-nil? [:attributes :http.route]]
                 (by {:fields [:service
                               [:attributes :http.route]
                               [:attributes :http.http.method]]}
                     (aggr-rate {:duration 10 :delay 10}
                                (with :service "rate"
                                      (publish! :websocket)))
                     (percentiles {:duration 10 :delay 5 :nb-significant-digits 3 :percentiles [0.99 0.5 0.75]}
                                  (with :service "stream-percentiles"
                                        (publish! :websocket)
                                        )

                                  )
                     (fixed-time-window {:duration 10}
                                        (coll-percentiles [0.99 0.5 0.75]
                                                          (with :service "percentiles"
                                                                (publish! :websocket))))))
    ))
