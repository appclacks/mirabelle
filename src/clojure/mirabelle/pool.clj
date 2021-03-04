(ns mirabelle.pool
  (:import java.util.concurrent.Executor
           java.util.concurrent.Executors
           java.util.concurrent.LinkedBlockingQueue
           java.util.concurrent.ThreadPoolExecutor
           java.util.concurrent.TimeUnit))

(defn shutdown
  "Graceful shutdown of an executor"
  [^Executor executor]
  (.shutdown executor)
  (.awaitTermination executor
                     (long (* 2 60 1000))
                     TimeUnit/MILLISECONDS))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn dynamic-thread-pool-executor
  "a ThreadPoolExecutor with core and
  maximum threadpool sizes, and a LinkedBlockingQueue of a given size. Options:

  - :core-pool-size             Default 1
  - :max-pool-size              Default 8
  - :keep-alive-time            Default 5000 (milliseconds)
  - :queue-size                 Default 1000"
  [{:keys [core-pool-size
           max-pool-size
           keep-alive-time
           queue-size]
    :or {core-pool-size 1
         max-pool-size 8
         keep-alive-time 5000
         queue-size 1000}}]
  (ThreadPoolExecutor.
   core-pool-size
   max-pool-size
   keep-alive-time
   TimeUnit/MILLISECONDS
   (LinkedBlockingQueue. ^int queue-size)))

(defn fixed-thread-pool-executor
  [size]
  (Executors/newFixedThreadPool size))
