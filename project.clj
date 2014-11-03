(defproject humongous-client "0.0.1-SNAPSHOT"
  :description "Mongo DB client"
  :dependencies [[org.clojure/clojure "1.6.0"]
                 [org.mongodb/mongo-java-driver "2.12.1"]
                 [org.clojure/core.incubator "0.1.3"]
                 [org.clojure/data.json "0.2.4"]]
  :profiles {:dev {:dependencies [[midje "1.6.3"]]}
             :1.4 {:dependencies [[org.clojure/clojure "1.4.0"]]}
             :1.5 {:dependencies [[org.clojure/clojure "1.5.1"]]}})
  
