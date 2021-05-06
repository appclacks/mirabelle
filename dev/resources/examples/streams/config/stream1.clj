(streams
 (stream
  {:name :foo}
  (where [:= :service "foo"]
    (info)
    (tap :foo)))
 (stream
  {:name :bar}
  (where [:> :metric 100]
    (info)
    (tap :bar))))


