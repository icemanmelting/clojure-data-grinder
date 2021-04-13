(ns clojure-data-grinder.config
  (:require [clojure.java.io :refer [resource as-file]]
            [aero.core :refer [read-config]]))

(def profile (keyword (System/getenv "CDG_ENV")))

(defn read-from-file [f & ks]
  (let [c (read-config f {:profile profile})]
    (if (seq ks)
      (get-in c ks)
      c)))

(defn read-from-resource [res & ks]
  (apply read-from-file (resource res) ks))

(defn read-from-file-or-resource [n & ks]
  (if (-> n as-file .exists)
    (apply read-from-file n ks)
    (apply read-from-resource n ks)))

(def conf (read-from-file-or-resource (or (System/getenv "CDG_CONFIG_FILE") "config.edn")))
