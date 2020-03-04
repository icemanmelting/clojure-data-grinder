(ns clojure-data-grinder.validation
  (:require [schema.core :as sc]
            [clojure.string :as cs]))

(def non-empty-str (sc/constrained sc/Str not-empty))

(def uuid-str (sc/constrained sc/Str (comp not nil? (partial re-matches #"[a-fA-F0-9]{8}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{4}-[a-fA-F0-9]{12}"))))

(defn validate [schema data]
  (try
    [(sc/validate schema data) nil]
    (catch Exception e [nil e])))

(defn humanize-error [e]
  (when e
    (cs/replace (.getMessage e) #":(\w+)" "\"$1\"")))
