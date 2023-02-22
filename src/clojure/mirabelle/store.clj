(ns mirabelle.store
  (:require [clojure.java.io :as io]
            [taoensso.nippy :as nippy]
            [byte-streams :as bs])
  (:import java.io.DataInputStream))

(defprotocol IStateStore
  (get! [this id default])
  (save! [this])
  (load! [this id]))

(defrecord MemoryStateStore []
  IStateStore
  (get! [_ _ default]
    (atom default))
  (save! [_])
  (load! [_ _ ]))

(defrecord FileStateStore [directory states]
  IStateStore
  (get! [_ id default]
    (let [state (atom default)]
      (swap! states assoc id state)
      state))
  (save! [_]
    (doseq [[id state] @states]
      (let [path (str directory "/" (name id))]
        (with-open [w (io/output-stream path)]
          (.write ^java.io.BufferedOutputStream w
                  ^"[B" (nippy/freeze @state))))))
  (load! [_ id]
    (let [path (str directory "/" (name id))
          content (nippy/thaw (bs/to-byte-array (java.io.File. path)))
          state (atom content)]
      (swap! states assoc id state)
      state
    )))

(comment
  (def store (FileStateStore. "/tmp/foo" (atom {})))
  (get! store :bar {:aaaa 1 :bbb 2})
  (save! store)
  (load! store :bar)
  )
