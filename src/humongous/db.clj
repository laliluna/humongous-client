(ns humongous.db
  (:import (com.mongodb MongoClient MongoClientURI DB WriteConcern)))

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

(def write-concerns
  {:acknowledged         WriteConcern/ACKNOWLEDGED
   :unacknowledged       WriteConcern/UNACKNOWLEDGED
   :fsynced              WriteConcern/FSYNCED
   :journaled            WriteConcern/JOURNALED
   :replica-acknowledged WriteConcern/REPLICA_ACKNOWLEDGED
   :majority             WriteConcern/MAJORITY})

(defn create-client
  ([]
   (MongoClient.))
  ([db-uri]
   (MongoClient. (MongoClientURI. db-uri))))

(defn translate-write-concern
  ([write-concern]
   (if-let [concern (get write-concerns write-concern)]
     concern
     (throw (IllegalArgumentException. (str "Unknown write concern: " write-concern))))))

(defn create-db-client
  [db-uri & {:keys [write-concern read-concern]}]
  (let [uri (MongoClientURI. db-uri)
        db (.getDB (MongoClient. uri) (.getDatabase uri))]
    (cond write-concern (.setWriteConcern db (translate-write-concern write-concern)))
    db))

(defmacro with-open! [bindings & body]
  `(let ~bindings
     (try
       ~@body
       (finally
         (close! ~(bindings 0))))))
