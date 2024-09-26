{:test
 {:default false,
  :actions
  {:action :sdo,
   :description {:message "Forward events to children"},
   :children
   ({:action :where,
     :description
     {:message "Filter events based on the provided condition",
      :params "[:= :kind \"client\"]"},
     :params [[:= :kind "client"]],
     :children
     ({:action :rate,
       :description
       {:message
        "Computes the rate of received events (by counting them) and emits it every null seconds"},
       :params [{:duration 10000000000, :aggr-fn :rate}],
       :children
       ({:action :with,
         :description
         {:message "Merge the events with the provided fields",
          :params "{:name \"RATE\"}"},
         :children
         ({:action :tap,
           :description {:message "Save events into the tap :result"},
           :params [:result]}
          {:action :info,
           :description
           {:message "Print the event in the logs as info"}}),
         :params [{:name "RATE"}]})})})}}}
