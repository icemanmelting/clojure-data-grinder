(ns clojure-data-grinder.core
  (:require [overtone.at-at :as at]
            [compojure.core :refer :all]
            [clojure-data-grinder-core.core :as core]
            [clojure-data-grinder.config :as c]
            [clojure-data-grinder.response :refer :all]
            [clojure.tools.logging :as log]
            [clojure.core.async :as a :refer [chan <!! put! mult tap close!]]
            [org.httpkit.server :refer [run-server]]
            [ring.middleware.json :as json :refer [wrap-json-response]]
            [ring.middleware.cors :as cors])
  (:import (sun.misc SignalHandler Signal)
           (clojure.lang Symbol)
           (clojure_data_grinder_core.core Step))
  (:gen-class))

(def ^:private executable-steps (atom {}))
(def channels (atom {}))
(def ^:private main-channel (chan 1))

(defn- pipelines->grouped-by-name
  "Groups pipelines by name"
  [pipelines]
  (group-by #(get-in % [:from :name]) pipelines))

(defn- ->pipeline-higher-buffer-size
  "Returns the pipelines with the higher buffer size.
  Used when there's a branching in the data pipeline,
  and a decision needs to be made to which one should be selected."
  [pipelines]
  (apply max-key
         #(get-in % [:from :buffer-size])
         pipelines))

(defn- create-channel
  "Creates a channel with ch-name as name of the channel, if it wasn't found in the channels atomic reference"
  [ch-name buffer-size]
  (if-let [ch (get @channels ch-name)]
    ch
    (do
      (log/debug "Starting channel " ch-name)
      (swap! channels assoc ch-name (chan buffer-size)))))

(defmulti bootstrap-pipeline
  "Multi method used to bootstrap a pipeline.
  The version of the method will depend on the amount of pipelines with the same name when grouped."
  (fn [entry]
    (log/info "Bootstraping pipeline!")
    (> (-> entry val count) 1)))

(defmethod bootstrap-pipeline false [entry]
  (log/debug "Bootstrapping pipeline" (key entry))
  (let [[{{f-name :name bs-from :buffer-size} :from {t-name :name bs-to :buffer-size} :to}] (val entry)]
    (create-channel f-name bs-from)
    (create-channel t-name bs-to)))

(defmethod bootstrap-pipeline true [entry]
  (log/info "Multiple output pipeline detected!")
  (let [{{f-name :name bs-from :buffer-size} :from} (->pipeline-higher-buffer-size (val entry))
        from-channel (create-channel f-name bs-from)
        mult-c (mult from-channel)]
    (doseq [c (val entry)]
      (let [{{t-name :name bs-to :buffer-size} :to} c]
        (create-channel t-name bs-to)
        (log/debug "Adding channel " t-name " to mult")
        (tap mult-c (get @channels t-name))))))

(defn- resolve-validation-function [v-fn]
  (when v-fn
    (require (symbol (.getNamespace v-fn)))
    (or (resolve v-fn) (throw (ex-info (str "Function " v-fn " cannot be resolved.") {})))))

(defn- instantiate-new-step-thread
  ([impl name conf v-fn in-ch fn out-ch pf]
   (instantiate-new-step-thread "" impl name conf v-fn in-ch fn out-ch pf))
  ([n impl name conf v-fn in-ch fn out-ch pf]
   (let [^Step s (impl {:state (atom {:processed-batches 0
                                      :successful-batches 0
                                      :unsuccessful-batches 0})
                        :name name
                        :conf conf
                        :v-fn v-fn
                        :in in-ch
                        :x-fn fn
                        :out out-ch
                        :poll-frequency-s pf})]
     (if v-fn
       (do (log/info "Validating Step " name)
           (.validate s))
       (log/info "Validation Function for Step " name "not present, skipping validation."))
     (log/info "Adding Step " name " to executable steps")
     (swap! executable-steps assoc (str name "-" n) s))))

(defn- bootstrap-step
  "Bootstraps a step. Validates and initializes it by resolving all functions or predefined types."
  [impl {name :name conf :conf ^Symbol v-fn :v-fn out :out in :in ^Symbol fn :fn type :type pf :poll-frequency-s threads :threads}]
  (require (symbol (.getNamespace (or fn type))))
  (let [in-ch (get @channels in)
        out-ch (get @channels out)
        v-fn (resolve-validation-function v-fn)
        fn (when (not type) (or (resolve fn) (throw (ex-info (str "Function " fn " cannot be resolved.") {}))))]
    (doseq [a (range (or threads 1))]
      (instantiate-new-step-thread a impl name conf v-fn in-ch fn out-ch pf))))

(defn- resolve-type->bootstrap-step
  "Resolves the type to instantiate and bootstraps the step."
  [col base-type]
  (doseq [{type :type :as s} col]
    (let [type (or type (symbol base-type))]
      (bootstrap-step (resolve type) s))))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;API ROUTES;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defn- wrap-json-body [h]
  (json/wrap-json-body h {:keywords? true
                          :malformed-response (response (error :unprocessable-entity "Wrong JSON format"))}))

(defn- wrap-cors [h]
  (cors/wrap-cors h
                  :access-control-allow-origin #".+"
                  :access-control-allow-methods [:get :put :post :delete :options]))

(defroutes main-routes
  (GET "/state" [] (render (fn [_]
                             (many :ok (for [s @executable-steps]
                                         {(key s) (.getState ^Step (val s))})))))
  (GET "/state/:name" [] (render (fn [{{:keys [name]} :params}]
                                   (if-let [^Step s (get @executable-steps name)]
                                     (one :ok (.getState s))
                                     (error :not-found "Please refer to an existing step")))))
  (PUT "/execution/stop" [] (fn [_]
                              (log/info "Stopping channels")
                              (put! main-channel :stop)
                              (one :ok {}))))

(defroutes app (-> main-routes wrap-json-body wrap-json-response wrap-cors))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defrecord KillSignalHandler []
  SignalHandler
  (^void handle [_ ^Signal _]
    (put! main-channel :stop)))

(defn- handle-int-signal []
  (Signal/handle (Signal. "INT") (->KillSignalHandler)))

(defn -main []
  (let [port (-> c/conf :api-server :port)]
    (run-server app {:port port})
    (log/info "Server started on port" port))
  (handle-int-signal)
  (let [{{sources :sources grinders :grinders sinks :sinks pipelines :pipelines} :steps} c/conf
        grouped-pipelines (pipelines->grouped-by-name pipelines)]
    (doseq [p grouped-pipelines]
      (bootstrap-pipeline p))
    (resolve-type->bootstrap-step sources "clojure-data-grinder-core.core/map->SourceImpl")
    (resolve-type->bootstrap-step grinders "clojure-data-grinder-core.core/map->GrinderImpl")
    (resolve-type->bootstrap-step sinks "clojure-data-grinder-core.core/map->SinkImpl")
    (doseq [[name step] @executable-steps]
      (log/info "Initializing Step " name)
      (if (instance? Runnable step)
        (.run ^Runnable step)
        (.init ^Step step)))
    (while true
      (let [v (<!! main-channel)]
        (if (= :stop v)
          (do (doseq [[name c] @channels]
                (log/info "Stopping channel " name)
                (close! c))
              (log/info "Stopping scheduled tasks")
              (core/reset-pool)
              (log/info "Shutting down...")
              (System/exit 0)))))))
