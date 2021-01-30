(ns mirabelle.time)

(defn now
  "Returns the current time in second"
  []
  (/ (double (System/currentTimeMillis)) 1000))

(def default-ttl 120)
