(ns mirabelle.graphviz
  (:require [mirabelle.stream :as stream]
            [clojure.string :as string]))


(defn action->graphviz
  [parent actions]
  (doall
   (for [action actions]
     (let [action-name (name (:action action))
           params (string/replace
                   (->> (:params action)
                        (map pr-str)
                        (string/join ", ")
                       )
                   #"\""
                   "'")
           node (str "a" (rand-int 1000000))
           label (format "%s [label = \"%s \n %s\"]" node action-name params)
           line (format "%s -> %s "
                        parent node)
           children (some->> (action->graphviz node
                                               (:children action))
                             seq
                             (clojure.string/join "\n"))]
       (str label "\n" line "\n" (or children ""))))))

(defn stream->graphviz
  [config]
  (format "digraph {\n %s \n }"
          (string/join
           "\n"
           (for [[stream-name config] config]
             (let [action (:actions config)]
               (str (format "subgraph %s {\n" (name stream-name))
                    (first (action->graphviz (name stream-name)
                                             [action]))
                    "\n}"))))))

(defn graphviz
  [streams-directories destination]
  (let [config (stream/read-edn-dirs streams-directories)]

    )
  )

(comment
  (do (def config (stream/read-edn-dirs ["/etc/mirabelle/streams"]))
      (spit "/tmp/graph/graph.dot" (stream->graphviz config)))
  )
