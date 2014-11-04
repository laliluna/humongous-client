(ns humongous-client.humongous
  (:import (com.mongodb BasicDBObject)
           (clojure.lang IPersistentVector IPersistentMap IPersistentList)
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

  IPersistentVector
  (to-mongo [v]
    (map to-mongo v))

  IPersistentList
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

  IPersistentVector
  (to-clojure [v]
    (map to-clojure v))

  IPersistentList
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

(defn- get-collection [coll]
  (assert *mongo-db-connection* "I miss an enclosing (with-db my-db ...)")
  (.getCollection *mongo-db-connection* (name coll)))

(defn drop! [coll]
  (.drop (get-collection coll)))

(defn- build-field-map [fields]
  (if fields
    (reduce (fn [m v] (assoc m v 1)) {} fields)))

(defn- build-sort-map [m]
  (if m
    (apply array-map
           (flatten (map (fn [v]
                   (if (vector? v)
                     [(first v)
                      (if (= (second v) :asc) 1 -1)]
                     [v 1])) m)))))

(defn fetch-docs
  ([coll]
   (fetch-docs coll {}))
  ([coll query & {:keys [fields sort-by] :or {fields nil :sort-by nil}}]
   (with-open [cursor (.find (get-collection coll)
                             (to-mongo query)
                             (to-mongo (build-field-map fields)))]
     (if sort-by
       (.sort cursor (to-mongo (build-sort-map sort-by))))
     (map to-clojure cursor))))

(defn insert! [coll data]
  (let [document (to-mongo data)]
    (.insert (get-collection coll)
             ^List (if (or (vector? document) (seq? document)) document (list document)))
    (to-clojure document)))

