(defproject mirabelle "0.1.0-SNAPSHOT"
  :description "A stream processing engine inspired by Riemann"
  :url "https://github.com/mcorbin/mirabelle"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[cc.qbits/tape "0.3.3"]
                 [io.netty/netty-all "4.1.58.Final"]
                 [io.netty/netty-codec "4.1.58.Final"]
                 [io.netty/netty-handler "4.1.58.Final"]
                 [io.netty/netty-buffer "4.1.58.Final"]
                 [io.netty/netty-common "4.1.58.Final"]
                 [io.netty/netty-transport "4.1.58.Final"]
                 [io.netty/netty-resolver "4.1.58.Final"]
                 [mcorbin/corbihttp "0.9.0"]
                 [org.apache.arrow/arrow-vector "3.0.0"]
                 [org.clojure/clojure "1.10.1"]
                 [org.clojure/spec.alpha "0.2.194"]
                 [riemann-clojure-client "0.5.1"]
                 [fr.mcorbin/mirabelle "1.0-SNAPSHOT"]]
  :main ^:skip-aot mirabelle.core
  :target-path "target/%s"
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java/src"]
  :profiles {:dev {:dependencies [[pjstadig/humane-test-output "0.10.0"]
                                  [org.clojure/tools.namespace "1.1.0"]]
                   :plugins [[lein-ancient "0.6.15"]
                             [lein-environ "1.1.0"]]
                   :source-paths ["dev"]
                   :repl-options {:init-ns user}
                   :env {:mirabelle-configuration "dev/resources/config.edn"}
                   :injections [(require 'pjstadig.humane-test-output)
                                (pjstadig.humane-test-output/activate!)]}
             :uberjar {:aot :all}})
