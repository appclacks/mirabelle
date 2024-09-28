{:percentiles
 {:default true,
  :actions
  {:action :sdo,
   :description {:message "Forward events to children"},
   :children
   ({:action :where,
     :description
     {:message "Filter events based on the provided condition",
      :params "[:= :name \"http_request_duration_seconds\"]"},
     :params [[:= :name "http_request_duration_seconds"]],
     :children
     ({:action :by,
       :description
       {:message
        "Split streams by field(s) [[:attributes :application] [:attributes :environment]]"},
       :params
       [{:fields
         [[:attributes :application] [:attributes :environment]]}],
       :children
       ({:action :percentiles,
         :description
         {:message "Computes the quantiles [0.5 0.75 0.99]"},
         :params
         [{:percentiles [0.5 0.75 0.99],
           :duration 20000000000,
           :nb-significant-digits 3}],
         :children
         ({:action :info,
           :description
           {:message "Print the event in the logs as info"}}
          {:action :where,
           :description
           {:message "Filter events based on the provided condition",
            :params "[:and [:= :quantile \"0.99\"] [:> :metric 1]]"},
           :params [[:and [:= :quantile "0.99"] [:> :metric 1]]],
           :children
           ({:action :with,
             :description
             {:message "Merge the events with the provided fields",
              :params "{:state \"error\"}"},
             :children
             ({:action :error,
               :description
               {:message "Print the event in the logs as error"}}
              {:action :tap,
               :description
               {:message "Save events into the tap :alert"},
               :params [:alert]}),
             :params [{:state "error"}]})})})})})}}}
