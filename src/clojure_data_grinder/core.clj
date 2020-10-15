(ns clojure-data-grinder.core
  (:require [compojure.core :refer :all]
            [clojure-data-grinder-core.core :refer :all]
            [clojure-data-grinder.config :as c]
            [clojure-data-grinder.response :refer :all]
            [clojure.tools.logging :as log]
            [clojure.core.async :as a :refer [chan <!! put! mult tap close!]]
            [org.httpkit.server :refer [run-server]]
            [ring.middleware.json :as json :refer [wrap-json-response]]
            [ring.middleware.cors :as cors])
  (:import (sun.misc SignalHandler Signal)
           (clojure.lang Symbol))
  (:gen-class))

(def ^:private executable-steps (atom {}))
(def  channels (atom {}))
(def ^:private main-channel (chan 1))

(defn- create-channels
  "Creates channels with ch-name as name of the channel, if it wasn't found in the channels atomic reference"
  [chs]
  (doseq [{name :name buffer-size :buffer-size} chs]
    (if-let [ch (get @channels name)]
      ch
      (do
        (log/debug "Starting channel " name)
        (swap! channels assoc name (chan buffer-size))))))

(defn- resolve-validation-function [v-fn]
  (when v-fn
    (require (symbol (.getNamespace v-fn)))
    (or (resolve v-fn) (throw (ex-info (str "Function " v-fn " cannot be resolved.") {})))))

(defn- instantiate-new-step-thread
  ([impl name conf v-fn in-ch x-fn out-chs pf]
   (instantiate-new-step-thread "" impl name conf v-fn in-ch x-fn out-chs pf))
  ([n impl name conf v-fn in-ch x-fn out-chs pf]
   (let [^Step s (impl {:state (atom {:processed-batches 0
                                      :successful-batches 0
                                      :unsuccessful-batches 0
                                      :stopped false})
                        :name name
                        :conf (atom conf)
                        :v-fn v-fn
                        :in in-ch
                        :x-fn x-fn
                        :out out-chs
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
        out-chs (-> (select-keys @channels out) vals vector flatten)
        v-fn (resolve-validation-function v-fn)
        fn (when (not type) (or (resolve fn) (throw (ex-info (str "Function " fn " cannot be resolved.") {}))))]
    (doseq [a (range (or threads 1))]
      (instantiate-new-step-thread a impl name conf v-fn in-ch fn out-chs pf))))

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
                                   (if-let [s (get @executable-steps name)]
                                     (one :ok (.getState ^Step s))
                                     (error :not-found "Please refer to an existing step")))))
  (PUT "/execution/stop" [] (fn [_]
                              (log/info "Stopping channels")
                              (put! main-channel :stop)
                              (one :ok {}))))

(defroutes app (-> main-routes wrap-json-body wrap-json-response wrap-cors))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;Kill Signal Handling;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord KillSignalHandler []
  SignalHandler
  (^void handle [_ ^Signal _]
    (put! main-channel :stop)))

(defn- handle-int-signal []
  (Signal/handle (Signal. "INT") (->KillSignalHandler)))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defn -main []
  (let [port (-> c/conf :api-server :port)]
    (run-server app {:port port})
    (log/info "Server started on port" port))
  (handle-int-signal)
  (let [{{chs :channels sources :sources grinders :grinders sinks :sinks} :steps} c/conf]
    (create-channels chs)
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
              (reset-pool)
              (log/info "Shutting down...")
              (System/exit 0)))))))
