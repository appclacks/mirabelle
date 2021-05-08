{:foo
 {:default true,
  :actions
  {:action :sdo,
   :children
   ({:action :where,
     :params [[:= :service "foo"]],
     :children ({:action :info} {:action :tap, :params [:foo]})})}},
 :bar
 {:default true,
  :actions
  {:action :sdo,
   :children
   ({:action :where,
     :params [[:= :service "bar"]],
     :children ({:action :error} {:action :tap, :params [:bar]})})}}}
