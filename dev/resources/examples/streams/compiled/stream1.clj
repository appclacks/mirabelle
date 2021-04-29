{:http_requests_duration
 {:actions
  {:action :sdo,
   :children
   ({:action :where,
     :params [[:= :service "http_requests_duration_seconds"]],
     :children
     ({:action :info}
      {:action :over,
       :params [1.5],
       :children
       ({:action :with,
         :children ({:action :error}),
         :params [{:state "critical"}]})})})}}}
