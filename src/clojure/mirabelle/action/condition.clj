(ns mirabelle.action.condition
  (:require [exoscale.ex :as ex]))

(def condition->fn
  "Map containing the functions associated to the where options"
  {:pos? pos?
   :neg? neg?
   :zero? zero?
   :> >
   :>= >=
   :< <
   :<= <=
   := =
   :always-true (constantly true)
   :contains (fn [field value]
               (some #(= value %) field))
   :absent (fn [field value] (not (some #( = value %) field)))
   :regex #(re-matches %2 %1)
   :nil? nil?
   :not-nil? (comp not nil?)
   :not= not=})

(defn valid-condition?
  [condition]
  (and
   (sequential? condition)
   (cond
     (or (= :or (first condition))
         (= :and (first condition)))
     (every? identity (map #(valid-condition? %) (rest condition)))

     (= :always-true (first condition))
     true

     :else
     (and ((-> condition->fn keys set)
           (first condition))
          (keyword? (second condition))))))

(defn compile-condition
  [[condition field & args]]
  (let [condition-fn (get condition->fn condition)
        regex? (= :regex condition)
        args (if (and regex? (string? (first args)))
               [(-> (first args) re-pattern)]
               args)]
    (when (nil? condition-fn)
      (ex/ex-incorrect! "Invalid condition" {}))
    (fn [event] (apply condition-fn
                       (get event field)
                       args))))

(def compile-conditions
  "Takes a condition and returns a function which can be applied to an
  event to check if the condition is valid for this event"
  (memoize
   (fn [conditions]
     (let [compile-conditions-fn
           (fn [cd] (reduce
                     (fn [state [operation :as condition]]
                       (if (or (= :and operation) (= :or operation))
                         (conj state (compile-conditions condition))
                         (conj state (compile-condition condition))))
                     []
                     cd))]
       (cond
         (= :or (first conditions))
         (let [cond-fns (compile-conditions-fn (rest conditions))]
           (fn [event] (some identity (map #(% event) cond-fns))))

         (= :and (first conditions))
         (let [cond-fns (compile-conditions-fn (rest conditions))]
           (fn [event] (every? identity (map #(% event) cond-fns))))

         :else
         (let [cond-fn (compile-condition conditions)]
           (fn [event] (cond-fn event))))))))
