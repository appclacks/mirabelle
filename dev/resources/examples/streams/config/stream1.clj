{:foo
 {:description "bar"
  :actions (where [:> :metric 10]
             (info)
             (increment
              (info)))}}
