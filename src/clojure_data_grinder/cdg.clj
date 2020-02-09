(ns clojure-data-grinder.cdg
  (:gen-class))

(defn source-file []
  [1 2 3 4 5 6 7 8 9 0])

(defn grinder-multiplier [v]
  (->> v (reduce +) (* 2)))

(defn sink-value [v]
  (prn v))
