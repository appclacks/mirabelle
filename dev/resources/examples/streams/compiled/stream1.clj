{:foo
 {:actions
  {:action :sdo,
   :children
   ({:action :where,
     :params [[:> :metric 10]],
     :children
     ({:action :increment,
       :children
       ({:action :with,
         :children
         ({:action :info} {:action :push-io!, :params [:influxdb]}),
         :params
         [{:influxdb/tags [:environment :state],
           :influxdb/fields [:host :metric]}]})})})}}}
