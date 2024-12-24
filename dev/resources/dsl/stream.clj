(streams
  (stream {:name :multiple-branches}
    (where [:= :name "http_request_duration_seconds"]
      (with :ttl 60
        ;; push everything into influxdb
        (output! :influxdb)
        ;; by will generate a branch for each :host value. Like that, downstream
        ;; computations will be per host and will not conflict between each other
        (by {:fields [:host]}
          ;; if the metric is greater than 1 for more than 60 seconds
          ;; Pass events downstream
          (above-dt {:duration 60 :threshold 1}
            ;; pass the state to critical
            (with :state "critical"
              ;; one alert only every 60 sec to avoid flooding pagerduty
              (throttle {:duration 60 :count 1}
                (output! :pagerduty)))))))))
