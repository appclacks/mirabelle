(ns mirabelle.pubsub
  (:require [com.stuartsierra.component :as component])
  (:import java.util.UUID))

(defprotocol IPubSub
  (add [this channel handler])
  (rm [this channel id])
  (publish! [this channel event]))

(defrecord PubSub [subscriptions]
  component/Lifecycle
  (start [this]
    (if subscriptions
      this
      (assoc this :subscriptions (atom {}))))
  (stop [this]
    (assoc this :subscriptions nil))
  IPubSub
  (add [_ channel handler]
    (let [id (UUID/randomUUID)]
      (swap! subscriptions assoc-in
             [(keyword channel)
              id]
             handler)
      id))
  (rm [_ channel id]
    (swap! subscriptions update (keyword channel) dissoc id))
  (publish! [_ channel event]
    (doseq [[_ handler] (get @subscriptions channel)]
      (handler event))))
