(streams
 (stream
  {:name :foo}
  (where [:> :metric 10]
         (info)
         (increment
          (tap :foo)
          (info)))))
