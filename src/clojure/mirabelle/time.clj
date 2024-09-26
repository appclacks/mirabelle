(ns mirabelle.time)

(defn now
  "Returns the current time in nanoseconds"
  []
  (* (System/currentTimeMillis) 1000000))

(def default-ttl 120)

(defn s->ns
  [seconds]
  (* seconds 1000000000))

(defn ms->ns
  [milliseconds]
  (* milliseconds 1000000))

(defn us->ns
  [microseconds]
  (* microseconds 1000))

(defn ns->ms
  [nanoseconds]
  (long (/ nanoseconds 1000000)))

(defn ns->s
  [nanoseconds]
  (long (/ nanoseconds 1000000000)))

(defn event-time-s->ns
  [event]
  (if (:time event)
    (update event :time s->ns)
    event))

(defn event-time-ns->s
  [event]
  (if (:time event)
    (update event :time ns->s)
    event))

(defn events-time-s->ns
  [events]
  (map event-time-s->ns events))
