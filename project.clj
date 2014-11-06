(defproject humongous-client "0.0.1"
  :description "Mongo DB client"
  :url "https://github.com/laliluna/humongous-client"
  :license {:name "Apache License Version 2.0"
            :url "http://www.apache.org/licenses/"}
  :min-lein-version "2.0.0"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.mongodb/mongo-java-driver "2.12.1"]
                 [org.clojure/core.incubator "0.1.3"]
                 [org.clojure/data.json "0.2.4"]]
  :profiles {:dev {:dependencies [[midje "1.6.3"]]
                   :plugins [[lein-midje "3.1.3"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.1"]]}
             :1.6 {:dependencies [[org.clojure/clojure "1.6.0"]]}}
  :aliases {"test-all"
            ["with-profile" "dev,1.5:dev,1.6" "midje"]}
  :deploy-repositories [["releases" :clojars]])
  
