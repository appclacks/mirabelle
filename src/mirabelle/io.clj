(ns mirabelle.io)

(defprotocol IO
  (compare-io [this other] "Compares two components and see if they are equal")
  (inject! [this event] "Inject an event in this IO component"))
