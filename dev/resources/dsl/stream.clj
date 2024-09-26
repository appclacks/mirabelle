(streams
  ;; (stream {:name :traces :default true}
  ;;         (where [:= :kind "client"]
  ;;                (rate {:duration 10}
  ;;                      (with {:name "RATE"}
  ;;                            (info)))))
(stream {:name :test :default false}
          (where [:= :kind "client"]
                 (rate {:duration 10}
                       (with {:name "RATE"}
                             (tap :result)
                             (info)))))
  )
