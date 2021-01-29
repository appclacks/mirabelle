(defproject mirabelle "0.1.0-SNAPSHOT"
  :description "A stream processing engine inspired by Riemann"
  :url "https://github.com/mcorbin/mirabelle"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[aero "1.1.6"]
                 [org.clojure/clojure "1.10.1"]
                 [org.clojure/tools.logging "1.1.0"]
                 [spootnik/unilog "0.7.27"]
                 [spootnik/signal "0.2.4"]]
  :main ^:skip-aot mirabelle.core
  :target-path "target/%s"
  :profiles {:dev {:dependencies [[pjstadig/humane-test-output "0.10.0"]]
                   :plugins [[lein-ancient "0.6.15"]]
                   :injections [(require 'pjstadig.humane-test-output)
                                (pjstadig.humane-test-output/activate!)]}
             :uberjar {:aot :all}})
