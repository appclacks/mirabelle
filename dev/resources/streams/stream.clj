{:multiple-branches
 {:actions
  {:action :sdo,
   :description {:message "Forward events to children"},
   :children
   ({:action :where,
     :description
     {:message "Filter events based on the provided condition",
      :params "[:= :name \"http_request_duration_seconds\"]"},
     :params [[:= :name "http_request_duration_seconds"]],
     :children
     ({:action :with,
       :description {:message "Set the field :ttl to 60"},
       :children
       ({:action :output!,
         :description
         {:message "Forward events to the output :influxdb"},
         :params [:influxdb]}
        {:action :by,
         :description {:message "Split streams by field(s) [:host]"},
         :params [{:fields [:host]}],
         :children
         ({:action :above-dt,
           :description
           {:message
            "Keep events if :metric is greater than 1 during 60 seconds"},
           :params [[:> :metric 1] 60000000000],
           :children
           ({:action :with,
             :description
             {:message "Set the field :state to critical"},
             :children
             ({:action :throttle,
               :description
               {:message "Let 1 events pass at most every 60 seconds"},
               :params [{:duration 60000000000, :count 1}],
               :children
               ({:action :output!,
                 :description
                 {:message "Forward events to the output :pagerduty"},
                 :params [:pagerduty]})}),
             :params [{:state "critical"}]})})}),
       :params [{:ttl 60}]})})}}}
