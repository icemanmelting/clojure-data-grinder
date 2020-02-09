(defproject clojure-data-grinder "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "EPL-2.0 OR GPL-2.0-or-later WITH Classpath-exception-2.0"
            :url  "https://www.eclipse.org/legal/epl-2.0/"}
  :dependencies [[org.clojure/clojure "1.10.0"]
                 [org.clojure/core.async "0.7.559"]
                 [org.clojure/tools.logging "0.6.0"]
                 [aero "1.1.4"]
                 [log4j/log4j "1.2.17"]]
  :main ^:skip-aot clojure-data-grinder.core
  :profiles {:uberjar {:aot :all}}
  :repl-options {:init-ns clojure-data-grinder.core})
