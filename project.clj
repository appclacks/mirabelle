(defproject fr.mcorbin/mirabelle "0.13.0"
  :description "A stream processing engine inspired by Riemann"
  :url "https://github.com/mcorbin/mirabelle"
  :license {:name "EPL-1.0"
            :url "https://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[clj-http "3.12.3"]
                 [com.boundary/high-scale-lib "1.0.6"]
                 [com.google.protobuf/protobuf-java "3.22.0"]
                 [com.google.protobuf/protobuf-java-util "3.22.0"]
                 [org.hdrhistogram/HdrHistogram "2.2.2"]
                 [com.taoensso/nippy "3.4.2"]
                 [io.netty/netty-all "4.1.89.Final"]
                 [io.netty/netty-codec "4.1.89.Final"]
                 [io.netty/netty-handler "4.1.89.Final"]
                 [io.netty/netty-buffer "4.1.89.Final"]
                 [io.netty/netty-common "4.1.89.Final"]
                 [io.netty/netty-transport "4.1.89.Final"]
                 [io.netty/netty-resolver "4.1.89.Final"]
                 [fr.mcorbin/corbihttp "0.33.0"]
                 [org.apache.kafka/kafka-clients "3.4.0"]
                 [org.clj-commons/byte-streams "0.3.2"]
                 [ch.qos.logback/logback-classic "1.2.11"]
                 [org.clojure/clojure "1.11.3"]
                 [org.clojure/spec.alpha "0.5.238"]
                 [org.elasticsearch.client/elasticsearch-rest-client "8.14.3"]
                 [org.xerial.snappy/snappy-java "1.1.10.5"]
                 [riemann-clojure-client "0.5.4"]]
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
                                  [org.clojure/tools.namespace "1.4.1"]
                                  [org.clojure/data.fressian "1.0.0"]
                                  [com.clojure-goes-fast/clj-memory-meter "0.2.2"]]
                   :plugins [[lein-ancient "0.6.15"]
                             [lein-environ "1.1.0"]]
                   :resource-paths ["resources" "test/resources" "gen-resources"]
                   :jvm-opts ["-Djdk.attach.allowAttachSelf"]
                   :repl-options {:init-ns user}
                   :env {:mirabelle-configuration "dev/resources/config.edn"}
                   :injections [(require 'pjstadig.humane-test-output)
                                (pjstadig.humane-test-output/activate!)]}
             :uberjar {:aot :all}})
