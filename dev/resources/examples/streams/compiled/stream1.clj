{:foo
 {:actions
  {:action :sdo,
   :children
   ({:action :async-queue!,
     :params [:thread-pool],
     :children ({:action :info})})}}}
