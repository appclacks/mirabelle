{:foo
 {:actions
  {:action :sdo,
   :children
   ({:action :exception-stream,
     :children
     ({:action :with,
       :children ({:action :increment, :children nil}),
       :params [{:metric "invalid!"}]}
      {:action :info})})}}}
