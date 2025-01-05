(streams
 (stream {:name :bar :default true}
  (where [:= :service "bar"]
    (publish! :my-channel))))
