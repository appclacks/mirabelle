;; Part of this code is from the Riemann code base
;; Copyright Riemann authors (riemann.io), thanks to them!
(ns mirabelle.io.pagerduty
  (:require [cheshire.core :as json]
            [clj-http.client :as client]
            [clojure.spec.alpha :as s]
            [clojure.string :as string]
            [com.stuartsierra.component :as component]
            [corbihttp.spec :as spec]
            [exoscale.cloak :as cloak]
            [exoscale.ex :as ex]
            [mirabelle.io :as io]
            [mirabelle.spec :as mspec]
            [mirabelle.time :as time]))

(def event-url "https://events.pagerduty.com/v2/enqueue")

(defn event+keys->str
  [event event-keys separator]
  (->> (reduce (fn [state k] (conj state (get event k)))
               []
               event-keys)
       (string/join separator)))

(defn post
  "POST to the PagerDuty events API."
  [request-body url options]
  (client/post url
               (merge
                {:body (json/generate-string request-body)
                 :socket-timeout 5000
                 :conn-timeout 5000
                 :content-type :json
                 :accept :json
                 :throw-entire-message? true}
                options)))

(defn parse-timestamp
  [event]
  (str (java.time.Instant/ofEpochSecond
        (or (:time event) (time/now)))))

(defn format-event
  "Formats an event for PagerDuty v2 API"
  [{:keys [source-key summary-keys]} event]
  {:summary (event+keys->str event summary-keys " - ")
   :source (get event source-key)
   :severity (condp = (:state event)
               "ok" "info"
               (:state event "critical"))
   :timestamp (parse-timestamp event)
   :custom_details event})

(defn request-body
  "Generate PD v2 API request body. event-action is one of :trigger, :acknowledge,
  :resolve"
  [{:keys [service-key dedup-keys] :as this} event-action event]
  (merge
   {:routing_key (cloak/unmask service-key)
    :event_action event-action
    :payload (format-event this event)}
   (if dedup-keys
     {:dedup_key (event+keys->str event dedup-keys "-")}
     {})))

(def valid-actions #{:trigger :acknowledge :resolve})

(defn send-event
  "Send an event to Pagerduty."
  [{:keys [http-options] :as component} event-action event]
  (when-not (valid-actions event-action)
    (ex/ex-incorrect! (format "Invalid Pagerduty action %s" event-action)
                      {:event event}))
  (post (request-body component
                      event-action
                      event)
        event-url
        http-options))

(s/def ::service-key ::spec/secret)
(s/def ::http-options (s/or :map map? :nil nil?))
(s/def ::source-key keyword?)
(s/def ::summary-keys (s/coll-of keyword? :kind (complement empty?)))
(s/def ::dedup-keys (s/coll-of keyword? :kind (complement empty?)))

(s/def ::pagerduty (s/keys :req-un [::service-key
                                    ::source-key
                                    ::summary-keys
                                    ::dedup-keys]
                           :opt-un [::http-options]))

(defrecord Pagerduty [service-key
                      http-options
                      source-key
                      summary-keys
                      dedup-keys]
  component/Lifecycle
  (start [this]
    (mspec/valid? ::pagerduty this))
  (stop [this] this)
  io/IO
  (inject! [this events]
    (doseq [event events]
      (send-event this
                  (or (:pagerduty/action event)
                      (condp = (:state event)
                        "critical" :trigger
                        "ok" :resolve
                        :trigger))
                  event))))
