(ns mirabelle.pool
  (:require [corbihttp.metric :as metric])
  (:import java.util.concurrent.Executor
           java.util.concurrent.Executors
           java.util.concurrent.LinkedBlockingQueue
           java.util.concurrent.ScheduledThreadPoolExecutor
           java.util.concurrent.ThreadPoolExecutor
           java.util.concurrent.TimeUnit))

(defn shutdown
  "Graceful shutdown of an executor"
  [^ThreadPoolExecutor executor]
  (when executor
    (.shutdown executor)
    (.awaitTermination executor
                       (long (* 2 60 1000))
                       TimeUnit/MILLISECONDS)))

(defn thread-pool-executor-metrics
  "Register metrics for a threadPoolExecutor"
  [registry ^ThreadPoolExecutor executor executor-name]
  (metric/gauge! registry
                 :executor.queue.remaining.capacity
                 {:executor executor-name}
                 (fn [] (.remainingCapacity
                         (.getQueue ^ThreadPoolExecutor executor))))
  (metric/gauge! registry
                 :executor.queue.tasks
                 {:executor executor-name
                  :state "completed"}
                 (fn [] (.getCompletedTaskCount executor)))
  (metric/gauge! registry
                 :executor.queue.tasks
                 {:executor executor-name
                  :state "accepted"}
                 (fn [] (.getTaskCount executor))))

;; Copyright Riemann authors (riemann.io), thanks to them!
(defn dynamic-thread-pool-executor
  "a ThreadPoolExecutor with core and
  maximum threadpool sizes, and a LinkedBlockingQueue of a given size. Options:

  - :core-pool-size             Default 1
  - :max-pool-size              Default 8
  - :keep-alive-time            Default 5000 (milliseconds)
  - :queue-size                 Default 10000"
  [registry
   executor-name
   {:keys [core-pool-size
           max-pool-size
           keep-alive-time
           queue-size]
    :or {core-pool-size 1
         max-pool-size 8
         keep-alive-time 5000
         queue-size 10000}}]
  (let [executor (ThreadPoolExecutor.
                  core-pool-size
                  max-pool-size
                  keep-alive-time
                  TimeUnit/MILLISECONDS
                  (LinkedBlockingQueue. ^int queue-size))]
    (thread-pool-executor-metrics registry executor executor-name)
    executor))

(defn fixed-thread-pool-executor
  [size]
  (Executors/newFixedThreadPool size))

(defn schedule!
  ^ScheduledThreadPoolExecutor
  [f {:keys [initial-delay-ms interval-ms]}]
  (let [executor (ScheduledThreadPoolExecutor. 1)]
    (.scheduleWithFixedDelay executor
                             ^Runnable f
                             (long initial-delay-ms)
                             (long interval-ms)
                             TimeUnit/MILLISECONDS)
    executor))
