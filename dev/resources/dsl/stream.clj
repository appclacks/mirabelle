(streams
  (stream {:name :test :default true}
          (sdissoc [[:attributes :x-client]]
                   (info)
                   (output! :prometheus))
          ))
