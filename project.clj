(defproject fr.mcorbin/mirabelle "0.5.0"
  :description "A stream processing engine inspired by Riemann"
  :url "https://github.com/mcorbin/mirabelle"
  :license {:name "EPL-1.0"
            :url "https://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[cc.qbits/tape "0.3.4"]
                 [clj-http "3.12.3"]
                 [com.boundary/high-scale-lib "1.0.6"]
                 [com.influxdb/influxdb-client-java "3.2.0"]
                 [exoscale/coax "1.0.0-alpha12"]
                 [http-kit "2.5.3"]
                 [io.netty/netty-all "4.1.58.Final"]
                 [io.netty/netty-codec "4.1.58.Final"]
                 [io.netty/netty-handler "4.1.58.Final"]
                 [io.netty/netty-buffer "4.1.58.Final"]
                 [io.netty/netty-common "4.1.58.Final"]
                 [io.netty/netty-transport "4.1.58.Final"]
                 [io.netty/netty-resolver "4.1.58.Final"]
                 [fr.mcorbin/corbihttp "0.17.0"]
                 [org.clojure/clojure "1.10.3"]
                 [org.clojure/spec.alpha "0.2.194"]
                 [org.elasticsearch.client/elasticsearch-rest-client "7.14.1"]
                 [riemann-clojure-client "0.5.1"]]
  :main ^:skip-aot mirabelle.core
  :global-vars {*warn-on-reflection* true}
  :target-path "target/%s"
  :source-paths ["src/clojure"]
  :plugins [[lein-codox "0.10.7"]]
  :codox {:source-uri "https://github.com/mcorbin/mirabelle/blob/{version}/{filepath}#L{line}"
          :output-path "site/api"
          :metadata   {:doc/format :markdown}}
  :profiles {:dev {:dependencies [[pjstadig/humane-test-output "0.11.0"]
                                  [org.clojure/tools.namespace "1.1.0"]
                                  [org.clojure/data.fressian "1.0.0"]
                                  [com.clojure-goes-fast/clj-memory-meter "0.1.3"]]
                   :source-paths ["dev"]
                   :plugins [[lein-ancient "0.6.15"]
                             [lein-environ "1.1.0"]]
                   :resource-paths ["resources" "test/resources" "gen-resources"]
                   :jvm-opts ["-Djdk.attach.allowAttachSelf"]
                   :repl-options {:init-ns user}
                   :env {:mirabelle-configuration "dev/resources/config.edn"}
                   :injections [(require 'pjstadig.humane-test-output)
                                (pjstadig.humane-test-output/activate!)]}
             :uberjar {:aot :all}})
