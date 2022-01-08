(ns mirabelle.leader
  (:require [clj-http.client :as http]
            [clojure.java.io :as io]
            [clojure.spec.alpha :as s]
            [com.stuartsierra.component :as component]
            [corbihttp.spec :as spec]
            [less.awful.ssl :as ssl]
            [mirabelle.spec :as mspec]))

(defprotocol LeaderElection
  (leader? [this] "Is this node leader ?")
  (election [this] "Performs a leader election")
  )

(def default-token-path "/var/run/secrets/kubernetes.io/serviceaccount/token")
(def default-cacert-path "/var/run/secrets/kubernetes.io/serviceaccount/ca.crt")

(s/def ::token ::spec/ne-string)
(s/def ::cacert ::spec/file-spec)
(s/def ::host ::spec/host)
(s/def ::port ::spec/port)
(s/def ::auth (s/keys :opt-un [::token ::cacert]))
(s/def ::id ::spec/ne-string)
(s/def ::namespace ::spec/ne-string)
(s/def ::cluster-id ::spec/ne-string)
(s/def ::kubernetes (s/keys :req-un [::id ::cluster-id ::host ::port ::namespace]
                            :opt-un [::auth]))

(defn lease-name
  [{:keys [cluster-id]}]
  (str "mirabelle-" cluster-id))

(defn take-lease-spec
  [{:keys [id lease-duration] :as config} lease]
  (let [ts (System/currentTimeMillis)]
    {:apiVersion "coordination.k8s.io"
     :kind "Lease"
     :metadata {:name (lease-name config)
                :namespace (:namespace config)}
     :spec {:acquireTime ts
            :holderIdentity id
            :leaseDurationSeconds lease-duration
            :leaseTransitions (inc (get-in lease [:spec :leaseTransitions]))
            :renewTime ts}}))

(defn renew-lease-spec
  [{:keys [id lease-duration] :as config} lease]
  (let [ts (System/currentTimeMillis)]
    {:apiVersion "coordination.k8s.io"
     :kind "Lease"
     :metadata {:name (lease-name config)
                :namespace (:namespace config)}
     :spec (merge (:spec lease)
                  {:leaseDurationSeconds lease-duration
                   :holderIdentity id
                   :renewTime ts})}))

(defn create-lease
  [{:keys [id lease-duration] :as config}]
  (let [ts (System/currentTimeMillis)]
    {:apiVersion "coordination.k8s.io"
     :kind "Lease"
     :metadata {:name (lease-name config)
                :namespace (:namespace config)}
     :spec {:acquireTime ts
            :holderIdentity id
            :leaseDurationSeconds lease-duration
            :leaseTransitions 0
            :renewTime ts}}))

(defn get-lease
  [{:keys [auth] :as config}]
  (http/get
   (format "https://%s:%d/apis/coordination.k8s.io/v1/namespaces/%s/leases/%s"
           (:host config)
           (:port config)
           (:namespace config)
           (lease-name config))
   (cond-> {:throw-exceptions false}
     (:token auth) (assoc :headers {:authorization (:token auth)})
     (:trust-store auth) (assoc :trust-store (:trust-store auth)))))

(defn kubernetes-election
  [config]
  (let [lease (get-lease config)]
    (if (= 404 (:status lease))
      (create-lease config)
      (cond
        (= (get-in lease [:spec :holderIdentity]) (:id config))
        "recreate the lease no matter what"
        "IF expired"
        "recreate as well"
        )
      )

    )


  )

(defrecord KubernetesElection [leader-status
                               cluster-id
                               id
                               host
                               port
                               lease-duration
                               namespace
                               auth]
  component/Lifecycle
  (start [this]
    (mspec/valid? ::kubernetes this)
    (let [default-token-file (io/file default-token-path)
          default-cacert-file (io/file default-cacert-path)
          cacert-path (or
                       (:cacert auth)
                       (when (and (.exists default-cacert-file)
                                  (.isFile default-cacert-file))
                         default-cacert-path)
                       default-cacert-path)
          token (or (:token auth)
                    (when (and (.exists default-token-file)
                               (.isFile default-token-file))
                      (slurp default-token-file)))
          trust-store (when cacert-path
                        (ssl/trust-store cacert-path))]
      (-> (assoc this :leader-status (atom false))
          (assoc-in [:auth :trust-store] trust-store)
          (assoc-in [:auth :token] token))))
  (stop [this]
    (assoc this :leader-status nil)
    )
  LeaderElection
  (leader? [this]
    @leader-status)
  (election [this]
    
    )
    )
