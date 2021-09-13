(ns mirabelle.graphviz
  (:require [mirabelle.stream :as stream]
            [clojure.string :as string]))


(defn action->graphviz
  [parent parent-params actions]
  (doall
   (for [action actions]
     (let [action-name (name (:action action))
           node (str "a" (rand-int 1000000))
           label (format "%s [label = \"%s\"]" node action-name)
           line (format "%s -> %s [label = \"%s\"]"
                        parent node
                        (if-let [params parent-params]
                          (clojure.string/replace (pr-str params)
                                                  #"\""
                                                  "'"
                                                  )
                          "next"))
           children (some->> (action->graphviz node
                                               (:params action)
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
                    (first (action->graphviz "entrypoint"
                                             ""
                                             [action]))
                    "\n}"))))))

(defn graphviz
  [streams-directories destination]
  (let [config (stream/read-edn-dirs streams-directories)]

    )
  )
