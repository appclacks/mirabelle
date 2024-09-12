{:test
 {:default true,
  :actions
  {:action :sdo,
   :description {:message "Forward events to children"},
   :children
   ({:action :sdissoc,
     :description
     {:message "Remove key(s) [[:attributes :x-client]] from events"},
     :params [[[:attributes :x-client]]],
     :children
     ({:action :info,
       :description {:message "Print the event in the logs as info"}}
      {:action :output!,
       :description
       {:message "Forward events to the output :prometheus"},
       :params [:prometheus]})})}}}
