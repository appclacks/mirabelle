(ns mirabelle.time)

(defn now
  "Returns the current time in nanoseconds"
  []
  (* (System/currentTimeMillis) 1000000))

(def default-ttl 120)
