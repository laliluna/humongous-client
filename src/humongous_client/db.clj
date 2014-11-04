(ns humongous-client.db
  (:import (com.mongodb MongoClient MongoClientURI DB)))

(defprotocol MongoConnection
  (close! [client]))

(extend-type MongoClient
  MongoConnection
  (close! [client]
    (.close client)))

(extend-type DB
  MongoConnection
  (close! [db]
    (.close (.getMongo db))))

(defn create-client
  ([]
   (MongoClient.))
  ([db-uri]
   (MongoClient. (MongoClientURI. db-uri))))

(defn create-db-client
  [db-uri]
  (let [uri (MongoClientURI. db-uri)]
    (.getDB (MongoClient. uri) (.getDatabase uri))))
