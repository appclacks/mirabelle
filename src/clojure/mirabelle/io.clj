(ns mirabelle.io)

(defprotocol IO
  (inject! [this event] "Inject an event in this IO component"))
