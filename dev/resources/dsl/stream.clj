(streams
  (stream {:name :test}
          ;(info)
          (by {:fields [[:attributes :http.route]
                        [:attributes :http.http.method]]}
              (aggr-rate {:duration 10 :delay 10}
                         (publish! :rate)
                         )
              )
          ;; (by {:fields [[:attributes :http.route]
          ;;               [:attributes :http.http.method]]}
          ;;     (fixed-time-window {:duration 30}
          ;;                        (coll-percentiles [0.5 0.75 0.98 0.99]
          ;;                                          (info))))
    ))
