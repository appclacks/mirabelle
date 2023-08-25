(streams
  (stream {:name :test}
          (where [:not-nil? [:attributes :http.route]]
                 (by {:fields [:service
                               [:attributes :http.route]
                               [:attributes :http.http.method]]}
                     (aggr-rate {:duration 10 :delay 10}
                                (with :service "rate"
                                      (publish! :websocket)))
                     (fixed-time-window {:duration 10}
                                        (coll-percentiles [0.99]
                                                          (with :service "percentiles"
                                                                (publish! :websocket))))))
    ))
