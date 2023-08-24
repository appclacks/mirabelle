{:test
 {:actions
  {:action :sdo,
   :description {:message "Forward events to children"},
   :children
   ({:action :by,
     :description
     {:message
      "Split streams by field(s) [[:attributes :http.route] [:attributes :http.http.method]]"},
     :params
     [{:fields
       [[:attributes :http.route] [:attributes :http.http.method]]}],
     :children
     ({:action :aggr-rate,
       :description
       {:message
        "Computes the rate of received events (by counting them) and emits it every null seconds"},
       :params [{:duration 10, :delay 10, :aggr-fn :rate}],
       :children
       ({:action :publish!,
         :description
         {:message "Publish events into the channel :rate"},
         :params [:rate],
         :children []})})})}}}
