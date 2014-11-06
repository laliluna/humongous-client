(ns humongous.db-test
  (:import (com.mongodb MongoClient))
  (:require [midje.sweet :refer :all]
            [humongous.db :refer :all]))

(fact "Connect and close database connection"
      (let [client (create-client)]
        (.getDatabaseNames client) => truthy
        (close! client)))

(fact "Use a URI to connect to the database"
      (let [client (create-client "mongodb://localhost:27017")]
        (.getDatabaseNames client) => truthy
        (close! client)))

(fact "Connect to a specific database"
      (let [db-client (create-db-client "mongodb://localhost:27017/test")]
        (.getCollectionNames db-client) => truthy
        (close! db-client)))

(fact "Connect to a specific database"
      (let [db-client (create-db-client "mongodb://localhost:27017/test")]
        (.getCollectionNames db-client) => truthy
        (close! db-client)))

(fact "Connect and define the default write and read concern"
      (let [db-client (create-db-client "mongodb://localhost:27017/test" :write-concern :acknowledged)]
        (.getCollectionNames db-client) => truthy
        (close! db-client)))
