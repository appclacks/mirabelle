{:test
 {:actions
  {:action :sdo,
   :description {:message "Forward events to children"},
   :children
   ({:action :info,
     :description {:message "Print the event in the logs as info"}})}}}
