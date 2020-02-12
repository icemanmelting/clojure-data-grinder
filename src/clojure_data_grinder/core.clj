(ns clojure-data-grinder.core
  (:require
    [overtone.at-at :as at]
    [compojure.core :refer :all]
    [clojure-data-grinder.config :as c]
    [clojure-data-grinder.response :refer :all]
    [clojure.tools.logging :as log]
    [clojure.core.async :as a :refer [chan go go-loop <!! <! put! pipeline mult tap close!]]
    [org.httpkit.server :refer [run-server]]
    [ring.middleware.json :as json :refer [wrap-json-response]]
    [ring.middleware.cors :as cors])
  (:import (sun.misc SignalHandler Signal))
  (:gen-class))

(def ^:private executable-steps (atom {}))
(def ^:private channels (atom {}))
(def ^:private main-channel (chan 1))

(defprotocol Step
  (init [this] "initialize the current step")
  (validate [this conf])
  (getState [this])
  (setState [this state-val]))

(defprotocol Source
  (output [this value] "method to output the sourced data"))

(def schedule-pool (at/mk-pool))

(defrecord SourceImpl [state name conf v-fn x-fn out poll-frequency-s]
  Source
  (output [this value]
    (log/debug "Adding value " value " to source channel " name)
    (put! out value))                                       ;;need to create logic for batch output, maybe method next batch???
  Step
  (init [this]
    (log/debug "Initialized Source " name)
    (at/every poll-frequency-s #(let [{sb :successful-batches ub :unsuccessful-batches pb :processed-batches :as s} @state]
                                  (try (output this (x-fn))
                                       (swap! s assoc :processed-batches (inc pb) :successful-batches (inc sb))
                                       (catch Exception e
                                         (log/error e)
                                         (swap! s assoc :processed-batches (inc pb) :unsuccessful-batches (inc ub))))) schedule-pool))
  (validate [this conf]
    (if-let [result (v-fn conf)]
      (throw (ex-info "Problem validating Source conf!" result))
      (log/debug "Source " name " validated")))
  (getState [this] @state)
  (setState [this state-val]
    (swap! state state-val)))

(defprotocol Grinder
  (grind [this v]))

(defrecord GrinderImpl [state name conf v-fn in x-fn out poll-frequency-s]
  Grinder
  (grind [this v]
    (log/debug "Grinding value " v " on Grinder " name)
    (put! out (x-fn v)))
  Step
  (init [this]
    (at/every poll-frequency-s #(let [v (<!! in)
                                      {sb :successful-batches ub :unsuccessful-batches pb :processed-batches :as s} @state]
                                  (try (grind this v)
                                       (swap! s assoc :processed-batches (inc pb) :successful-batches (inc sb))
                                       (catch Exception e
                                         (log/error e)
                                         (swap! s assoc :processed-batches (inc pb) :unsuccessful-batches (inc ub))))
                                  ) schedule-pool))
  (validate [this conf]
    (if-let [result (v-fn conf)]
      (throw (ex-info "Problem validating Grinder conf!" result))
      (log/debug "Grinder " name " validated")))
  (getState [this] @state)
  (setState [this state-val]
    (swap! state state-val)))

(defprotocol Sink
  (sink [this v] "method to sink data"))

(defrecord SinkImpl [state name conf v-fn x-fn in poll-frequency-s]
  Sink
  (sink [this v]
    (log/debug "Sinking value " v " to " name)
    (x-fn v))
  Step
  (validate [this conf]
    (if-let [result (v-fn conf)]
      (throw (ex-info "Problem validating Sink conf!" result))
      (log/debug "Sink " name " validated")))
  (init [this]
    (prn @state)
    (at/every poll-frequency-s #(let [v (<!! in)
                                      {sb :successful-batches ub :unsuccessful-batches pb :processed-batches} @state]
                                  (try
                                    (sink this v)
                                    (swap! state assoc :processed-batches (inc pb) :successful-batches (inc sb))
                                    (catch Exception e
                                      (log/error e)
                                      (swap! state assoc :processed-batches (inc pb) :unsuccessful-batches (inc ub))))) schedule-pool))
  (getState [this] @state)
  (setState [this state-val]
    (swap! state state-val)))

(defn- pipelines->grouped-by-name [pipelines]
  (group-by #(get-in % [:from :name]) pipelines))

(defn- ->pipeline-higher-buffer-size [pipelines]
  (apply max-key
         #(get-in % [:from :buffer-size])
         (-> pipelines first second)))

(defmulti bootstrap-pipeline (fn [entry] (> (-> entry val count) 1)))

(defmethod bootstrap-pipeline false [entry]
  (log/debug "Bootstrapping pipeline" (key entry))
  (let [[{{f-name :name bs-from :buffer-size} :from {t-name :name bs-to :buffer-size} :to}] (val entry)]
    (when-not (get @channels f-name)
      (log/debug "Starting from channel " f-name)
      (swap! channels assoc f-name (chan bs-from)))
    (when-not (get @channels t-name)
      (log/debug "Starting to channel " t-name)
      (swap! channels assoc t-name (chan bs-to)))))

(defmethod bootstrap-pipeline true [entry]
  (log/debug "Bootstrapping pipeline!")
  (log/debug "Multiple output pipeline detected!")
  (let [{{f-name :name bs-from :buffer-size} :from} (->pipeline-higher-buffer-size (val entry))
        from-channel (chan bs-from)
        mult-c (mult from-channel)]
    (when-not (get @channels f-name)
      (log/debug "Starting from channel " f-name)
      (swap! channels assoc f-name from-channel))
    (doseq [c (val entry)]
      (let [{{t-name :name bs-to :buffer-size} :to} c]
        (if-not (get @channels t-name)
          (let [to-channel (chan bs-to)]
            (log/debug "Starting to channel " t-name)
            (swap! channels assoc t-name (chan bs-to))
            (log/debug "Adding to channel " t-name "to mult")
            (tap mult-c to-channel))
          (let [to-channel (get @channels t-name)]
            (log/debug "To channel " f-name " already exists!")
            (log/debug "Adding to channel " t-name " to mult")
            (tap mult-c to-channel)))))))

(defn- bootstrap-step [impl {name :name conf :conf v-fn :v-fn out :out in :in fn :fn pf :poll-frequency-s}]
  (require (symbol (.getNamespace fn)))
  (let [in-ch (get @channels in)
        out-ch (get @channels out)
        v-fn (when v-fn
               (require (symbol (.getNamespace v-fn)))
               (or (resolve v-fn) (throw (ex-info (str "Function " v-fn " cannot be resolved.") {}))))
        fn (or (resolve fn) (throw (ex-info (str "Function " fn " cannot be resolved.") {})))
        s (impl {:state            (atom {:processed-batches    0
                                          :successful-batches   0
                                          :unsuccessful-batches 0})
                 :name             name
                 :conf             conf
                 :v-fn             v-fn
                 :in               in-ch
                 :x-fn             fn
                 :out              out-ch
                 :poll-frequency-s pf})]
    (if v-fn
      (do (log/info "Validating Step " ~name)
          (.validate s conf))
      (log/info "Validation Function for Source " name "not present, skipping validation."))
    (log/info "Adding source " name " to executable steps")
    (swap! executable-steps assoc name s)))

(defrecord KillSignalHandler []
  SignalHandler
  (^void handle [this ^Signal signal]
    (put! main-channel :stop)))

(defn- wrap-json-body [h]
  (json/wrap-json-body h {:keywords?          true
                          :malformed-response (response (error :unprocessable-entity "Wrong JSON format"))}))

(defn- wrap-cors [h]
  (cors/wrap-cors h
                  :access-control-allow-origin #".+"
                  :access-control-allow-methods [:get :put :post :delete :options]))

(defroutes main-routes
           (GET "/state" [] (render (fn [_]
                                      (many :ok (for [s @executable-steps]
                                                  {(key s) (.getState (val s))})))))
           (GET "/state/:name" [] (render (fn [{{:keys [name]} :params}]
                                            (if-let [s (get @executable-steps name)]
                                              (one :ok (.getState s))
                                              (error :not-found {:message "Please refer to an existing step"})))))
           (PUT "/execution/stop" [] (fn [_]
                                       (put! main-channel :stop)
                                       (one :ok {}))))

(defroutes app (-> main-routes wrap-json-body wrap-json-response wrap-cors))

(defn -main []
  (run-server app {:port 8080})
  (log/info "Server started on port 8080")                  ;;customize port?
  (Signal/handle (Signal. "INT") (new KillSignalHandler))
  (let [{{sources :sources grinders :grinders sinks :sinks pipelines :pipelines} :steps} c/conf
        grouped-pipelines (pipelines->grouped-by-name pipelines)]
    (doseq [p grouped-pipelines]
      (bootstrap-pipeline p))
    (doseq [s sources]
      (bootstrap-step map->SourceImpl s))
    (doseq [g grinders]
      (bootstrap-step map->GrinderImpl g))
    (doseq [s sinks]
      (bootstrap-step map->SinkImpl s))

    (doseq [s (vals @executable-steps)]
      (log/info "Initializing Step " (:name s))
      (.init s))

    (while true
      (let [v (<!! main-channel)]
        (if (= :stop v)
          (do (doseq [[name c] @channels]
                (log/info "Stopping channel " name)
                (close! c))
              (log/info "Shutting down...")
              (System/exit 0)))))))
;;todo - add atomic state of every step, and save it once the shutdown is called, so that
;;todo - it can be loaded once it is started back up again