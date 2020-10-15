(defproject clojure-data-grinder "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url  "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/core.async "0.7.559"]
                 [org.clojure/data.json "0.2.7"]
                 [org.clojure/tools.logging "0.6.0"]
                 [clojure-data-grinder-core "0.1.0-SNAPSHOT"]
                 [aero "1.1.4"]
                 [clojure-data-grinder-core "0.1.0-SNAPSHOT" :exclusions [core.async tools.logging at-at dirwatch log4j]]
                 [overtone/at-at "1.2.0"]
                 [juxt/dirwatch "0.2.5"]
                 [log4j/log4j "1.2.17"]
                 [http-kit "2.3.0"]
                 [compojure "1.6.1" :exclusions [clj-time]]
                 [prismatic/schema "1.1.12"]
                 [ring/ring-json "0.5.0" :exclusions [clj-time]]
                 [ring-cors "0.1.13"]]
  :main ^:skip-aot clojure-data-grinder.core
  :profiles {:uberjar {:aot :all}}
  :repl-options {:init-ns clojure-data-grinder.core})
