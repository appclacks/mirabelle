(ns mirabelle.io.elasticsearch
  (:require [cheshire.core :as json]
            [com.stuartsierra.component :as component]
            [corbihttp.log :as log]
            [corbihttp.spec :as spec]
            [clojure.spec.alpha :as s]
            [exoscale.cloak :as cloak]
            [less.awful.ssl :as less-ssl]
            [mirabelle.b64 :as b64]
            [mirabelle.config :as config]
            [mirabelle.io :as io]
            [mirabelle.spec :as mspec]
            [mirabelle.time :as time])
  (:import java.time.Instant
           java.time.format.DateTimeFormatter
           java.time.ZoneOffset
           org.apache.http.client.config.RequestConfig$Builder
           org.apache.http.auth.AuthScope
           org.apache.http.auth.UsernamePasswordCredentials
           org.apache.http.entity.ContentType
           org.apache.http.impl.client.BasicCredentialsProvider
           org.apache.http.impl.nio.client.HttpAsyncClientBuilder
           org.apache.http.impl.nio.reactor.IOReactorConfig
           org.apache.http.nio.entity.NStringEntity
           org.apache.http.HttpHost
           org.apache.http.message.BasicHeader
           org.elasticsearch.client.Request
           org.elasticsearch.client.ResponseListener
           org.elasticsearch.client.RestClient
           org.elasticsearch.client.RestClientBuilder
           org.elasticsearch.client.RestClientBuilder$HttpClientConfigCallback
           org.elasticsearch.client.RestClientBuilder$RequestConfigCallback))

(s/def ::address ::spec/ne-string)
(s/def ::port pos-int?)
(s/def ::host (s/keys :req-un [::address ::port]))
(s/def ::hosts (s/coll-of ::host :min-count 1))
(s/def ::scheme #{"http" "https"})
(s/def ::default-index ::spec/ne-string)
(s/def ::default-index-pattern ::spec/ne-string)
(s/def ::id ::spec/ne-string)
(s/def ::secret ::spec/secret)
(s/def ::api-key (s/keys :req-un [::id ::secret]))
(s/def ::username ::spec/ne-string)
(s/def ::password ::spec/secret)
(s/def ::service-token ::spec/secret)
(s/def ::basic-auth (s/keys :req-un [::username ::password]))
(s/def ::elasticsearch (s/keys :req-un [::hosts
                                        ::default-index-pattern
                                        ::default-index
                                        ::scheme]
                               :opt-un [::config/cacert
                                        ::config/cert
                                        ::config/key
                                        ::connect-timeout
                                        ::socket-timeout
                                        ::thread-count
                                        ::basic-auth
                                        ::service-token
                                        ::api-key]))


(defn config->http-hosts
  [config]
  (->> config
       :hosts
       (map (fn [host]
              (HttpHost. ^String (:address host)
                         ^Integer (:port host)
                         ^String (:scheme config))))))

(defn request-config-callback
  [{:keys [connect-timeout socket-timeout]}]
  (reify RestClientBuilder$RequestConfigCallback
    (^RequestConfig$Builder customizeRequestConfig [_ ^RequestConfig$Builder builder]
     (when connect-timeout
       (.setConnectTimeout builder connect-timeout))
     (when socket-timeout
       (.setSocketTimeout builder socket-timeout))
      builder)))

(defn http-config-callback
  [{:keys [cacert cert thread-count basic-auth] :as config}]
  (reify RestClientBuilder$HttpClientConfigCallback
    (^HttpAsyncClientBuilder customizeHttpClient [_ ^HttpAsyncClientBuilder builder]
     (when thread-count
       (let [reactor-builder (IOReactorConfig/custom)]
         (.setIoThreadCount reactor-builder thread-count)
         (.setDefaultIOReactorConfig builder
                                     (.build reactor-builder))))

     (when basic-auth
       (let [provider (BasicCredentialsProvider.)]
         (.setCredentials provider AuthScope/ANY (UsernamePasswordCredentials.
                                                  (:username basic-auth)
                                                  (cloak/unmask (:password basic-auth))))
         (.setDefaultCredentialsProvider builder provider)))
     (when cacert
       (let [ssl-context (less-ssl/ssl-context (:key config)
                                               cert
                                               cacert)]
         (.setSSLContext builder ssl-context)))
     builder)))

(defn create-client
  [{:keys [service-token api-key] :as config}]
  (let [hosts ^"[Lorg.apache.http.HttpHost;" (into-array (config->http-hosts config))
        ^RestClientBuilder builder (RestClient/builder hosts)]
    (.setRequestConfigCallback builder (request-config-callback config))
    (.setHttpClientConfigCallback builder (http-config-callback config))
    (when service-token
      (.setDefaultHeaders builder
                          (into-array [(BasicHeader. "Authorization"
                                                     (str "Bearer " (cloak/unmask service-token)))])))
    (when api-key
      (let [secret (.getBytes ^String (b64/to-base64 (str (:id api-key)
                                                          ":"
                                                          (cloak/unmask (:secret api-key))))
                              java.nio.charset.StandardCharsets/UTF_8)]
        (.setDefaultHeaders builder
                            (into-array [(BasicHeader. "Authorization"
                                                       (str "ApiKey " secret))]))))
    (.build builder)))

(defn format-event
  [{:keys [default-index default-index-formatter]} event]
  (let [time (.atZone (Instant/ofEpochMilli (* 1000 (:time event (time/now))))
                      ZoneOffset/UTC)
        index (format "%s-%s"
                      (:elasticsearch/index event default-index)
                      (.format ^DateTimeFormatter default-index-formatter
                               time))]
    (format "{\"index\":{\"_index\":\"%s\"}}\n%s\n"
            index
            (json/generate-string
             (-> event
                 (dissoc :time :elasticsearch/index)
                 (assoc "@timestamp" (str time)))))))

(defn format-events
  [config events]
  (reduce #(format "%s%s" %1 (format-event config %2)) "" events))

(defrecord Elasticsearch [config
                          ^RestClient client
                          ^ResponseListener response-listener]
  component/Lifecycle
  (start [this]
    (mspec/valid? ::elasticsearch config)
    (if client
      this
      (-> (assoc this
                 :client (create-client config)
                 :response-listener (reify ResponseListener
                                      (onSuccess [_ _])
                                      (^void onFailure [_ ^Exception e]
                                       (log/error
                                        {}
                                        e
                                        "Elasticsearch error"))))
          (assoc-in [:config :default-index-formatter]
                    (DateTimeFormatter/ofPattern (:default-index-pattern config))))))
  (stop [this]
    (when client
      (.close ^RestClient client))
    (assoc this :client nil :response-listener nil))
  io/IO
  (inject! [_ events]
    (let [^Request request (Request. "POST" "/_bulk")]
      (.setEntity request
                  (NStringEntity. ^String (format-events config events)
                                  ^ContentType ContentType/APPLICATION_JSON))
      (.performRequestAsync client request response-listener))))
