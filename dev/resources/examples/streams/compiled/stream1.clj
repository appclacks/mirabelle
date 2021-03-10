{:foo
 {:description "bar",
  :actions
  {:action :where,
   :params [[:> :metric 10]],
   :children
   ({:action :info}
    {:action :increment,
     :children ({:action :tap, :params [:foo]} {:action :info})})}}}
