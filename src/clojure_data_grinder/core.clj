(ns clojure-data-grinder.core
  (:require [overtone.at-at :as at]
            [compojure.core :refer :all]
            [clojure-data-grinder.config :as c]
            [clojure-data-grinder.response :refer :all]
            [clojure.tools.logging :as log]
            [clojure.core.async :as a :refer [chan go go-loop <!! put! mult tap close!]]
            [juxt.dirwatch :refer [watch-dir]]
            [org.httpkit.server :refer [run-server]]
            [ring.middleware.json :as json :refer [wrap-json-response]]
            [ring.middleware.cors :as cors])
  (:import (sun.misc SignalHandler Signal)
           (clojure.lang Symbol))
  (:gen-class))

(def ^:private executable-steps (atom {}))
(def ^:private channels (atom {}))
(def ^:private main-channel (chan 1))
(def ^:private schedule-pool (at/mk-pool))

(defprotocol Step
  "Base step that contains the methods common to all Steps in the processing pipeline"
  (init [this] "initialize the current step")
  (validate [this])
  (getState [this]))

(defprotocol Source
  "Source Step -> reads raw data ready to be processed"
  (output [this value] "method to output the sourced data"))

(defrecord SourceImpl [state name conf v-fn x-fn out poll-frequency-s]
  Source
  (output [this value]
    (log/debug "Adding value " value " to source channel " name)
    (put! out value))                                       ;;todo - need to create logic for batch output, maybe method next batch???
  Step
  (init [this]
    (log/debug "Initialized Source " name)
    (at/every poll-frequency-s
              #(let [{sb :successful-batches ub :unsuccessful-batches pb :processed-batches} @state]
                 (try (output this (x-fn))
                      (swap! state merge {:processed-batches (inc pb) :successful-batches (inc sb)})
                      (catch Exception e
                        (log/error e)
                        (swap! state merge {:processed-batches (inc pb) :unsuccessful-batches (inc ub)}))))
              schedule-pool))
  (validate [this]
    (if-let [result (v-fn conf)]
      (throw (ex-info "Problem validating Source conf!" result))
      (log/debug "Source " name " validated")))
  (getState [this] @state))

;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;Pre-implemented sources;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;
(defrecord FileWatcherSource [state name conf v-fn x-fn out poll-frequency-s]
  Source
  (output [this value]
    (log/debug "Adding value " value " to source channel " name)
    (put! out value))
  Step
  (init [this]
    (log/debug "Initialized Source " name)
    (watch-dir #(put! out %) (clojure.java.io/file (:watch-dir conf))))
  (validate [this]
    (if-let [result (v-fn conf)]
      (throw (ex-info "Problem validating Source conf!" result))
      (log/debug "Source " name " validated")))
  (getState [this] @state))
;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;;

(defprotocol Grinder
  "Data Grinder -> processes the raw data"
  (grind [this v]))

(defrecord GrinderImpl [state name conf v-fn in x-fn out poll-frequency-s]
  Grinder
  (grind [this v]
    (log/debug "Grinding value " v " on Grinder " name)
    (when-let [res (x-fn v)]
      (put! out res)))
  Step
  (init [this]
    (at/every poll-frequency-s
              #(let [v (<!! in)
                     {sb :successful-batches ub :unsuccessful-batches pb :processed-batches} @state]
                 (try (grind this v)
                      (swap! state merge {:processed-batches (inc pb) :successful-batches (inc sb)})
                      (catch Exception e
                        (log/error e)
                        (swap! state merge {:processed-batches (inc pb) :unsuccessful-batches (inc ub)}))))
              schedule-pool))
  (validate [this]
    (if-let [result (v-fn conf)]
      (throw (ex-info "Problem validating Grinder conf!" result))
      (log/debug "Grinder " name " validated")))
  (getState [this] @state))

(defprotocol Sink
  "Data Sink -> sinks the data into whatever form needed, DB, File, Cloud, etc"
  (sink [this v] "method to sink data"))

(defrecord SinkImpl [state name conf v-fn x-fn in poll-frequency-s]
  Sink
  (sink [this v]
    (log/debug "Sinking value " v " to " name)
    (x-fn v))
  Step
  (validate [this]
    (if-let [result (v-fn conf)]
      (throw (ex-info "Problem validating Sink conf!" result))
      (log/debug "Sink " name " validated")))
  (init [this]
    (at/every poll-frequency-s
              #(let [v (<!! in)
                     {sb :successful-batches ub :unsuccessful-batches pb :processed-batches} @state]
                 (try
                   (sink this v)
                   (swap! state merge {:processed-batches (inc pb) :successful-batches (inc sb)})
                   (catch Exception e
                     (log/error e)
                     (swap! state merge {:processed-batches (inc pb) :unsuccessful-batches (inc ub)}))))
              schedule-pool))
  (getState [this] @state))

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
         (-> pipelines first second)))

(defmulti bootstrap-pipeline
          "Multi method used to bootstrap a pipeline.
          The version of the method will depend on the amount of pipelines with the same name when grouped."
          (fn [entry] (> (-> entry val count) 1)))

(defn- create-channel
  "Creates a channel with ch-name as name of the channel, if it wasn't found in the channels atomic reference"
  [ch-name buffer-size]
  (when-not (get @channels ch-name)
    (log/debug "Starting channel " ch-name)
    (swap! channels assoc ch-name (chan buffer-size))))

(defmethod bootstrap-pipeline false [entry]
  (log/debug "Bootstrapping pipeline" (key entry))
  (let [[{{f-name :name bs-from :buffer-size} :from {t-name :name bs-to :buffer-size} :to}] (val entry)]
    (create-channel f-name bs-from)
    (create-channel t-name bs-to)))

(defmethod bootstrap-pipeline true [entry]
  (log/debug "Bootstrapping pipeline!")
  (log/debug "Multiple output pipeline detected!")
  (let [{{f-name :name bs-from :buffer-size} :from} (->pipeline-higher-buffer-size (val entry))
        from-channel (create-channel f-name bs-from)
        mult-c (mult from-channel)]
    (doseq [c (val entry)]
      (let [{{t-name :name bs-to :buffer-size} :to} c
            to-channel (create-channel t-name bs-to)]
        (log/debug "Adding channel " t-name " to mult")
        (tap mult-c to-channel)))))

(defn- bootstrap-step
  "Bootstraps a step. Validates and initializes it by resolving all functions or predefined types."
  [impl {name :name conf :conf ^Symbol v-fn :v-fn out :out in :in ^Symbol fn :fn type :type pf :poll-frequency-s}]
  (when (not type)
    (require (symbol (.getNamespace fn))))
  (let [in-ch (get @channels in)
        out-ch (get @channels out)
        v-fn (when v-fn
               (require (symbol (.getNamespace v-fn)))
               (or (resolve v-fn) (throw (ex-info (str "Function " v-fn " cannot be resolved.") {}))))
        fn (when (not type) (or (resolve fn) (throw (ex-info (str "Function " fn " cannot be resolved.") {}))))
        s (impl {:state (atom {:processed-batches 0
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
      (do (log/info "Validating Step " ~name)
          (validate s))
      (log/info "Validation Function for Source " name "not present, skipping validation."))
    (log/info "Adding source " name " to executable steps")
    (swap! executable-steps assoc name s)))

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
                                                  {(key s) (getState (val s))})))))
           (GET "/state/:name" [] (render (fn [{{:keys [name]} :params}]
                                            (if-let [s (get @executable-steps name)]
                                              (one :ok (getState s))
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

(defn -main []
  (let [port (-> c/conf :api-server :port)]
    (run-server app {:port port})
    (log/info "Server started on port" port))
  (Signal/handle (Signal. "INT") (->KillSignalHandler))
  (let [{{sources :sources grinders :grinders sinks :sinks pipelines :pipelines} :steps} c/conf
        grouped-pipelines (pipelines->grouped-by-name pipelines)]
    (doseq [p grouped-pipelines]
      (bootstrap-pipeline p))
    (resolve-type->bootstrap-step sources "clojure-data-grinder.core/map->SourceImpl")
    (resolve-type->bootstrap-step grinders "clojure-data-grinder.core/map->GrinderImpl")
    (resolve-type->bootstrap-step sinks "clojure-data-grinder.core/map->SinkImpl")
    (doseq [s (vals @executable-steps)]
      (log/info "Initializing Step " (:name s))
      (init s))
    (while true
      (let [v (<!! main-channel)]
        (if (= :stop v)
          (do (doseq [[name c] @channels]
                (log/info "Stopping channel " name)
                (close! c))
              (log/info "Stopping scheduled tasks")
              (at/stop-and-reset-pool! schedule-pool :strategy :kill)
              (log/info "Shutting down...")
              (System/exit 0)))))))
