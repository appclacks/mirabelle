(streams
  (stream {:name :test}
          ;(info)
          (fixed-time-window
           (ddt)
           )
          ;; (by {:fields [[:attributes :http.route]
          ;;               [:attributes :http.http.method]]}
          ;;     (fixed-time-window {:duration 30}
          ;;                        (coll-percentiles [0.5 0.75 0.98 0.99]
          ;;                                          (info))))
    ))
