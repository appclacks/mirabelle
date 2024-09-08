(ns mirabelle.output.batch
  (:require [com.stuartsierra.component :as component]
            [corbihttp.log :as log]
            [mirabelle.event :as e]
            [mirabelle.pool :as pool]
            [mirabelle.time :as time])
  (:import java.util.concurrent.ScheduledThreadPoolExecutor))

(defprotocol Batcher
  (inject! [this event]))

(defrecord SimpleBatcher [max-duration-ns
                          initial-delay-ns
                          max-size
                          downstream
                          state
                          ^ScheduledThreadPoolExecutor pool]
  Batcher
  (inject! [this event]
    (let [events (e/sequential-events event)]
      (let [{:keys [to-send]}
            (swap! state
                   (fn [{:keys [items last-flush to-send] :as state}]
                     (if (>= (+ (count items) (count events))
                             max-size)
                       (-> (assoc state
                                  :items []
                                  :last-flush (System/nanoTime)
                                  :to-send (apply conj items events)))
                       (-> (assoc state :items (apply conj items events))
                           (assoc :to-send nil)))))]
        (when (pos? (count to-send))
          (downstream to-send)))))
  component/Lifecycle
  (start [this]
    (let [state (atom {:items []
                       :last-flush (System/nanoTime)
                       :to-send nil})]
      (assoc this
             :state state
             :pool (pool/schedule!
                    (fn []
                      (try
                        (let [{:keys [to-send]}
                              (swap! state
                                     (fn [{:keys [items last-flush to-send] :as state}]
                                       (let [now (System/nanoTime)]
                                         (if (and
                                              (pos? (count items))
                                              (> now
                                                 (+ last-flush max-duration-ns)))
                                           (assoc state
                                                  :items []
                                                  :last-flush now
                                                  :to-send items)
                                           (assoc state :to-send nil)))))]
                          (when (pos? (count to-send))
                            (downstream to-send)))
                        (catch Exception e
                          (log/error {} e "Batcher error"))))
                    {:initial-delay-ms (time/ns->ms max-duration-ns)
                     :interval-ms (time/ns->ms max-duration-ns)}))))
  (stop [this]
    (when pool
      (.shutdown pool))
    (when-let [items (:items @state)]
      (when downstream
        (downstream items)))))
