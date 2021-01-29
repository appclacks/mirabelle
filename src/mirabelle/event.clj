(ns mirabelle.event)

(defn most-recent
  "Get the most recent event from an event list"
  [events]
  (-> (sort-by :time events)
      last))
