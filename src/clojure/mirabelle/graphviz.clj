(ns mirabelle.graphviz
  (:require [mirabelle.stream :as stream]
            [clojure.string :as string])
  (:import java.util.UUID))

(defn action->graphviz
  [stream-name parent actions]
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
           node (string/replace (str "a" (UUID/randomUUID))
                                #"-"
                                "")
           label (format "%s [label = \"%s \n %s\"];" node action-name params)
           line (cond-> (format "%s -> %s;"
                                parent node)
                  (= :reinject! (:action action))
                  (str (format "\n%s -> \"%s entrypoint\""
                               node
                               (or (some-> (:params action) first name)
                                   stream-name))))
           children (some->> (action->graphviz stream-name
                                               node
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
               (str
                (if (:default config)
                  (str "default -> \"" (name stream-name)
                       " entrypoint\";\n")
                  "")
                (format "subgraph cluster_%s {\nlabel = \"Stream %s\n%s\";\n"
                        (name stream-name)
                        (name stream-name)
                        (:description config ""))
                    (first (action->graphviz
                            (name stream-name)
                            (format "\"%s entrypoint\""
                                    (name stream-name))
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
