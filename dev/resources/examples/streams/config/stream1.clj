{:foo
 {:description "bar"
  :actions
  (where [:> :metric 10]
         (info)
         (increment
          (tap :foo)
          (info)))}}
