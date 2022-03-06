(defproject fr.mcorbin/mirabelle "0.9.0"
  :description "A stream processing engine inspired by Riemann"
  :url "https://github.com/mcorbin/mirabelle"
  :license {:name "EPL-1.0"
            :url "https://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[clj-http "3.12.3"]
                 [com.boundary/high-scale-lib "1.0.6"]
                 [com.google.protobuf/protobuf-java "3.19.4"]
                 [com.google.protobuf/protobuf-java-util "3.19.4"]
                 [com.influxdb/influxdb-client-java "4.3.0"]
                 [io.netty/netty-all "4.1.58.Final"]
                 [io.netty/netty-codec "4.1.58.Final"]
                 [io.netty/netty-handler "4.1.58.Final"]
                 [io.netty/netty-buffer "4.1.58.Final"]
                 [io.netty/netty-common "4.1.58.Final"]
                 [io.netty/netty-transport "4.1.58.Final"]
                 [io.netty/netty-resolver "4.1.58.Final"]
                 [fr.mcorbin/corbihttp "0.28.0"]
                 [org.clojure/clojure "1.10.3"]
                 [org.clojure/spec.alpha "0.3.218"]
                 [org.elasticsearch.client/elasticsearch-rest-client "7.14.1"]
                 [org.xerial.snappy/snappy-java "1.1.8.4"]
                 [riemann-clojure-client "0.5.3"]]
  :main ^:skip-aot mirabelle.core
  :global-vars {*warn-on-reflection* true}
  :target-path "target/%s"
  :source-paths ["src/clojure"]
  :java-source-paths ["src/java"]
  :plugins [[lein-codox "0.10.8"]]
  :codox {:source-uri "https://github.com/mcorbin/mirabelle/blob/{version}/{filepath}#L{line}"
          :output-path "site/mirabelle/static/generated-doc"
          :metadata   {:doc/format :markdown}}
  :profiles {:dev {:dependencies [[pjstadig/humane-test-output "0.11.0"]
                                  [org.clojure/tools.namespace "1.2.0"]
                                  [org.clojure/data.fressian "1.0.0"]
                                  [com.clojure-goes-fast/clj-memory-meter "0.1.3"]]
                   :plugins [[lein-ancient "0.6.15"]
                             [lein-environ "1.1.0"]]
                   :resource-paths ["resources" "test/resources" "gen-resources"]
                   :jvm-opts ["-Djdk.attach.allowAttachSelf"]
                   :repl-options {:init-ns user}
                   :env {:mirabelle-configuration "dev/resources/config.edn"}
                   :injections [(require 'pjstadig.humane-test-output)
                                (pjstadig.humane-test-output/activate!)]}
             :uberjar {:aot :all}})
