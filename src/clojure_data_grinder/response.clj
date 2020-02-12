(ns clojure-data-grinder.response
  (:import (java.util Date)))

(def codes
  {:continue 100
   :switching-protocols 101
   :processing 102
   :ok 200
   :created 201
   :accepted 202
   :non-authoritative-information 203
   :no-content 204
   :reset-content 205
   :partial-content 206
   :multi-status 207
   :im-used 226
   :multiple-choices 300
   :moved-permanently 301
   :found 302
   :see-other 303
   :not-modified 304
   :use-proxy 305
   :temporary-redirect 307
   :bad-request 400
   :unauthorized 401
   :payment-required 402
   :forbidden 403
   :not-found 404
   :method-not-allowed 405
   :not-acceptable 406
   :proxy-authentication-required 407
   :request-timeout 408
   :conflict 409
   :gone 410
   :length-required 411
   :precondition-failed 412
   :request-entity-too-large 413
   :request-uri-too-long 414
   :unsupported-media-type 415
   :requested-range-not-satisfiable 416
   :expectation-failed 417
   :authentication-timeout 419
   :unprocessable-entity 422
   :locked 423
   :failed-dependency 424
   :upgrade-required 426
   :internal-server-error 500
   :not-implemented 501
   :bad-gateway 502
   :service-unavailable 503
   :gateway-timeout 504
   :http-version-not-supported 505
   :insufficient-storage 507
   :not-extended 510})

(defn one
  ([st ts d]
   (array-map :status (or (codes st) st) :data (apply array-map (apply concat d))))
  ([st d]
   (one st (Date.) d)))

(defn many
  ([st ts col offset total]
   (let [c (count col)
         next (if offset (+ offset c))]
     {:data (map #(apply array-map (apply concat %)) col) :status (or (codes st) st)}))
  ([st col offset total]
   (many st (Date.) col offset total))
  ([st col]
   (many st (Date.) col nil nil)))

(defn error [st m]
  (one st {:api_message m}))

(defn response [{b :data st :status}]
  {:body b :status (or st 200)})

(defn render [h]
  #(response(h %)))

