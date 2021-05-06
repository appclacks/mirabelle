{:foo
 {:actions
  {:action :sdo,
   :children
   ({:action :where,
     :params [[:= :service "foo"]],
     :children ({:action :info} {:action :tap, :params [:foo]})})}},
 :bar
 {:actions
  {:action :sdo,
   :children
   ({:action :where,
     :params [[:> :metric 100]],
     :children ({:action :info} {:action :tap, :params [:bar]})})}}}
