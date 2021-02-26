(ns mirabelle.pool
  (:import java.util.concurrent.Executors
           java.util.concurrent.LinkedBlockingQueue
           java.util.concurrent.ThreadPoolExecutor
           java.util.concurrent.TimeUnit))

(defn thread-pool-executor
  [{:keys [core-pool-size
           max-pool-size
           keep-alive-time
           queue-size]}]
  (ThreadPoolExecutor.
   core-pool-size
   max-pool-size
   keep-alive-time
        TimeUnit/MILLISECONDS
        (LinkedBlockingQueue. ^int queue-size)))

(defn fixed-thread-pool-executor
  [size]
  (Executors/newFixedThreadPool size))
