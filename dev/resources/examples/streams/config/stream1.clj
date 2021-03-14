(streams
 (stream
  {:name :foo}
  (where [:> :metric 10]
         (info)
         (index [:host :service])
         (increment
          (tap :foo)
          (info)))))
