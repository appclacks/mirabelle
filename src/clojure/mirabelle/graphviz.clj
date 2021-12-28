(ns mirabelle.graphviz
  (:require [mirabelle.stream :as stream]
            [clojure.string :as string])
  (:import java.util.UUID))

(defn escape
  "Change special characters into HTML character entities."
  [text]
  (.. ^String text
    (string/replace "&" "&amp;")
    (string/replace "<" "&lt;")
    (string/replace ">" "&gt;")
    (string/replace "\"" "&quot;")))

;; worst code ever written
(defn action->graphviz
  [stream-name parent actions subgraph-connections]
  (doall
   (for [action actions]
     (let [action-name (name (:action action))
           params (escape (or (get-in action [:description :params]) ""))
           description (escape (get-in action [:description :message]))
           node (string/replace (str "a" (UUID/randomUUID))
                                #"-"
                                "")
           label (format "%s [label=<<B>%s</B> <BR/><BR/> %s <BR/> %s>];"
                         node
                         action-name
                         description
                         (if (= "" params)
                           params                                                                               (str "<BR/>" params)))
           _ (when (= :reinject! (:action action))
               (swap! subgraph-connections str (format "\n%s -> \"%s entrypoint\""
                                                       node
                                                       (or (some-> (:params action)
                                                                   first name)
                                                           stream-name))))
           line (format "%s -> %s;"
                        parent node)
           children (some->> (action->graphviz stream-name
                                               node
                                               (:children action)
                                               subgraph-connections)
                             seq
                             (clojure.string/join "\n"))]
       (str label "\n" line "\n" (or children ""))))))

(defn stream->graphviz
  [config]
  (let [subgraph-connections (atom "")]
    (format "digraph {\nnode[shape=box];\n %s \n %s }"
            (string/join
             "\n"
             (for [[stream-name config] config]
               (let [action (:actions config)]
                 (str
                  (if (:default config)
                    (str "default -> \"" (name stream-name)
                         " entrypoint\";\n")
                    "")
                  (format "subgraph cluster_%s {\nlabel =<<B>Stream %s</B><BR/>%s>;\nlabeljust=\"l\";\n"
                          (name stream-name)
                          (name stream-name)
                          (:description config ""))
                  (first (action->graphviz
                          (name stream-name)
                          (format "\"%s entrypoint\""
                                  (name stream-name))
                          [action]
                          subgraph-connections))
                  "\n}"))))
            @subgraph-connections)))

(defn graphviz
  [streams-directories destination]
  (let [config (stream/read-edn-dirs streams-directories)
        result (stream->graphviz config)]
    (spit destination result)))

(comment
  (do (def config (stream/read-edn-dirs ["/etc/mirabelle/streams"]))
      (spit "/tmp/graph/graph.dot" (stream->graphviz config))))
