(defproject
 humongous-client "0.0.4-SNAPSHOT"
 :description "Mongo DB client"
 :url "https://github.com/laliluna/humongous-client"
 :license {:name "Apache License Version 2.0"
           :url "http://www.apache.org/licenses/"}
 :min-lein-version "2.0.0"
 :dependencies [[org.clojure/clojure "1.6.0"]
                [org.mongodb/mongo-java-driver "2.13.2"]
                [org.clojure/core.incubator "0.1.3"]
                [org.clojure/data.json "0.2.4"]]
 :profiles {:dev {:dependencies [[midje "1.6.3"]]
                  :plugins [[lein-midje "3.1.3"] [lein-cprint "1.2.0"]]}
            :1.6 {:dependencies [[org.clojure/clojure "1.6.0"]]}
            :1.7 {:dependencies [[org.clojure/clojure "1.7.0"]]}
            :1.8 {:dependencies [[org.clojure/clojure "1.8.0"]]}}

 :aliases {"test-all"
           ["with-profile" "dev,1.6:dev,1.7:dev,1.8" "midje"]}
 :deploy-repositories [["releases" :clojars]]
 :release-tasks
 [["vcs" "assert-committed"]
  ["change" "version" "leiningen.release/bump-version" "release"]
  ["vcs" "commit"]
  ["vcs" "tag" "--no-sign"]
  ["deploy"]
  ["change" "version" "leiningen.release/bump-version"]
  ["vcs" "commit"]
  ["vcs" "push"]])
