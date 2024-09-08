(ns mirabelle.output.http
  (:import org.apache.hc.client5.http.impl.async.HttpAsyncClients
           org.apache.hc.client5.http.impl.async.CloseableHttpAsyncClient))

(defn ^CloseableHttpAsyncClient async-client
  [_]
  (let [builder (HttpAsyncClients/custom)
        client (.build builder)]
    (.start client)
    client))
