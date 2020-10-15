(ns clojure-data-grinder.config
  (:require [clojure.java.io :refer [resource as-file]]
            [aero.core :refer [read-config]]
            [schema.core :as sc]
            [clojure-data-grinder.validation :refer [non-empty-str]]))

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

;(def ^:private contacts-update-fmt {(sc/optional-key :first_name) non-empty-str
;                                    (sc/optional-key :last_name) non-empty-str})

(def conf (read-from-resource (or (System/getenv "CDG_CONFIG_FILE") "config.edn")))
