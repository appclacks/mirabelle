(ns mirabelle.storage.rocksdb
  (:require [taoensso.nippy :as nippy])
  (:import org.rocksdb.RocksDB
           org.rocksdb.RocksDBException
           org.rocksdb.Options
           ))


(defn create-db
  [path]
  (RocksDB/loadLibrary)
  (let [options (.setCreateIfMissing (Options. ) true)]
    (RocksDB/open path)
    )
  )

(defn write-event
  [db event]
  
  )
