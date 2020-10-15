#!/usr/bin/env bash
BASEDIR=$(dirname "$0")

cp "/mnt/c/Users/iCeMan/IdeaProjects/cdg-test/target/cdg-test-0.1.0-SNAPSHOT-standalone.jar" $BASEDIR
cp  "/mnt/c/Users/iCeMan/IdeaProjects/clojure-data-grinder/target/clojure-data-grinder-0.1.0-SNAPSHOT-standalone.jar" $BASEDIR
java -cp  clojure-data-grinder-0.1.0-SNAPSHOT-standalone.jar::cdg-test-0.1.0-SNAPSHOT-standalone.jar clojure_data_grinder.core
