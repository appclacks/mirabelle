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
         :children ({:action :info}),
         :params
         [{:influxdb/tags [:environment :state :host],
           :influxdb/fields [:metric]}]})})})}}}
