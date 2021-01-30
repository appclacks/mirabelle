(defproject mirabelle "0.1.0-SNAPSHOT"
  :description "A stream processing engine inspired by Riemann"
  :url "https://github.com/mcorbin/mirabelle"
  :license {:name "EPL-2.0"
            :url "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[mcorbin/corbihttp "0.9.0"]
                 [org.clojure/clojure "1.10.1"]
                 [org.clojure/spec.alpha "0.2.194"]]
  :main ^:skip-aot mirabelle.core
  :target-path "target/%s"
  :profiles {:dev {:dependencies [[pjstadig/humane-test-output "0.10.0"]]
                   :plugins [[lein-ancient "0.6.15"]]
                   :injections [(require 'pjstadig.humane-test-output)
                                (pjstadig.humane-test-output/activate!)]}
             :uberjar {:aot :all}})
