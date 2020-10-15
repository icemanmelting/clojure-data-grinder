(ns clojure-data-grinder.core
  (:require [overtone.at-at :as at]
            [compojure.core :refer :all]
            [clojure-data-grinder.config :as c]
            [clojure-data-grinder.response :refer :all]
            [clojure-data-grinder-core.core :refer :all]
            [clojure.tools.logging :as log]
            [clojure.core.async :as a :refer [chan go go-loop <!! put! mult tap close!]]
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

(defn- bootstrap-step [impl {name :name conf :conf ^Symbol v-fn :v-fn out :out in :in ^Symbol fn :fn pf :poll-frequency-s}]
  (require (symbol (.getNamespace fn)))
  (let [in-ch (get @channels in)
        out-ch (get @channels out)
        v-fn (when v-fn
               (require (symbol (.getNamespace v-fn)))
               (or (resolve v-fn) (throw (ex-info (str "Function " v-fn " cannot be resolved.") {}))))
        fn (or (resolve fn) (throw (ex-info (str "Function " fn " cannot be resolved.") {})))
        ^Step s (impl {:state (atom {:processed-batches 0
                                     :successful-batches 0
                                     :unsuccessful-batches 0
                                     :stopped false})
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
    (doseq [s sources]
      (bootstrap-step map->SourceImpl s))
    (doseq [g grinders]
      (bootstrap-step map->GrinderImpl g))
    (doseq [s sinks]
      (bootstrap-step map->SinkImpl s))
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
