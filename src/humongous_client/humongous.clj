(ns humongous-client.humongous
  (:import (com.mongodb BasicDBObject)
           (clojure.lang IPersistentVector IPersistentMap)
           (java.util List)
           (org.bson.types ObjectId)))

(def ^{:dynamic true} *mongo-db-connection* nil)

(defmacro with-db [db-connection & body]
  `(binding [*mongo-db-connection* ~db-connection]
     ~@body))

(defprotocol ConvertToMongo
  "Protocol to be implemented by all classes which should be stored into a database"
  (to-mongo [v]))

(extend-protocol ConvertToMongo
  IPersistentMap
  (to-mongo [v]
    (let [r (BasicDBObject.)]
      (doseq [[k v] v]
        (.put r (name k) (to-mongo v)))
      r))

  List
  (to-mongo [v]
    (map to-mongo v))

  nil
  (to-mongo [v]
    v)

  Object
  (to-mongo [v]
    v))

(defprotocol ConvertToClojure
  "Protocol to be implemented by all classes which should be stored into a database"
  (to-clojure [v]))

(extend-protocol ConvertToClojure
  BasicDBObject
  (to-clojure [map]
    (reduce (fn [m [k v]] (assoc m (keyword k) (to-clojure v))) {}
            (.entrySet map)))
  List
  (to-clojure [v]
    (map to-clojure v))
  ObjectId
  (to-clojure [v]
    (str v))
  nil
  (to-clojure [v]
    v)
  Object
  (to-clojure [v] v))

(defn drop! [coll]
  (.drop (.getCollection *mongo-db-connection* (name coll))))

(defn fetch-docs [coll]
  (with-open [cursor (.find (.getCollection *mongo-db-connection* (name coll)))]
    (map to-clojure cursor)))

(defn insert! [coll data]
  (let [document (to-mongo data)]
    (.insert (.getCollection *mongo-db-connection* (name coll))
             (list document))
    (to-clojure document)))

