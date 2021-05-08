(streams
 (stream
  {:name :foo :default true}
  (where [:= :service "foo"]
    (info)
    (tap :foo)))
 (stream
  {:name :bar :default true}
  (where [:= :service "bar"]
    (info)
    (tap :bar))))


