{:foo
 {:actions
  {:action :sdo,
   :children
   ({:action :where,
     :params [[:> :metric 10]],
     :children
     ({:action :increment,
       :children
       ({:action :push-io!, :params [:influxdb]} {:action :info})})})}}}
