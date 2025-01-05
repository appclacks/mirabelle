{:bar
 {:default true,
  :actions
  {:action :sdo,
   :description {:message "Forward events to children"},
   :children
   ({:action :where,
     :description
     {:message "Filter events based on the provided condition",
      :params "[:= :service \"bar\"]"},
     :params [[:= :service "bar"]],
     :children
     ({:action :publish!,
       :description
       {:message "Publish events into the channel :my-channel"},
       :params [:my-channel],
       :children []})})}}}
