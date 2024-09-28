;; (streams
;;   ;; (stream {:name :traces :default true}
;;   ;;         (where [:= :kind "client"]
;;   ;;                (rate {:duration 10}
;;   ;;                      (with {:name "RATE"}
;;   ;;                            (info)))))
;; ;; (stream {:name :test :default false}
;; ;;           (where [:= :kind "client"]
;; ;;                  (rate {:duration 10}
;; ;;                        (with {:name "RATE"}
;; ;;                              (tap :result)
;; ;;                              (info)))))

;;   (stream {:name :percentiles })
;;   )

(streams
 (stream {:name :percentiles :default true}
         (where [:= :name "http_request_duration_seconds"]

                (by {:fields [[:attributes :application]
                              [:attributes :environment]]}
                    (percentiles {:percentiles [0.5 0.75 0.99]
                                  :duration 20
                                  :nb-significant-digits 3}
                                 (info)
                                 (where [:and [:= :quantile "0.99"]
                                         [:> :metric 1]]
                                        (with {:state "error"}
                                              (error)
                                              (tap :alert))))))))
