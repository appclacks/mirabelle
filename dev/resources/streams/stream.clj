{:test
 {:actions
  {:action :sdo,
   :description {:message "Forward events to children"},
   :children
   ({:action :where,
     :description
     {:message "Filter events based on the provided condition",
      :params "[:not-nil? [:attributes :http.route]]"},
     :params [[:not-nil? [:attributes :http.route]]],
     :children
     ({:action :by,
       :description
       {:message
        "Split streams by field(s) [:service [:attributes :http.route] [:attributes :http.http.method]]"},
       :params
       [{:fields
         [:service
          [:attributes :http.route]
          [:attributes :http.http.method]]}],
       :children
       ({:action :aggr-rate,
         :description
         {:message
          "Computes the rate of received events (by counting them) and emits it every null seconds"},
         :params [{:duration 10, :delay 10, :aggr-fn :rate}],
         :children
         ({:action :with,
           :description {:message "Set the field :service to rate"},
           :children
           ({:action :publish!,
             :description
             {:message "Publish events into the channel :websocket"},
             :params [:websocket],
             :children []}),
           :params [{:service "rate"}]})}
        {:action :percentiles,
         :description {:message "Computes the quantiles"},
         :params
         [{:duration 10,
           :delay 5,
           :nb-significant-digits 3,
           :percentiles [0.99 0.5 0.75]}],
         :children
         ({:action :with,
           :description
           {:message "Set the field :service to stream-percentiles"},
           :children
           ({:action :publish!,
             :description
             {:message "Publish events into the channel :websocket"},
             :params [:websocket],
             :children []}),
           :params [{:service "stream-percentiles"}]})}
        {:action :fixed-time-window,
         :description {:message "Build 10 seconds fixed time windows"},
         :params [{:duration 10, :aggr-fn :fixed-time-window}],
         :children
         ({:action :coll-percentiles,
           :description
           {:message
            "Computes percentiles for quantiles [0.99 0.5 0.75]"},
           :params [[0.99 0.5 0.75]],
           :children
           ({:action :with,
             :description
             {:message "Set the field :service to percentiles"},
             :children
             ({:action :publish!,
               :description
               {:message "Publish events into the channel :websocket"},
               :params [:websocket],
               :children []}),
             :params [{:service "percentiles"}]})})})})})}}}
