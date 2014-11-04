(ns humongous-client.humongous
  (:import (com.mongodb BasicDBObject)
           (clojure.lang IPersistentVector IPersistentMap IPersistentList)
           (java.util List)
           (org.bson.types ObjectId)
           (java.util.concurrent TimeUnit)))

(def ^{:dynamic true} *mongo-db-connection* nil)

(defmacro with-db
  "Binds the connection to a dynamic var
  Sample:
  -------
  (with-db db (fetch-docs :kites))"
  [db-connection & body]
  `(binding [*mongo-db-connection* ~db-connection]
     ~@body))

(defprotocol ConvertToMongo
  "Protocol to be implemented by all types which should be stored into the database"
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
  "Protocol to be implemented by all types which should be retrieved from the database"
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

(defn drop!
  "Sample:
  --------
  Drop a collection:
  (with-db db (drop! :kites))"
  [coll]
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

(def valid-params [:fields :sort-by :limit :skip :comment :batch-size :max-time-millis])

(defn fetch-docs
  "Sample:
   --------
   Fetch all:
      (with-db db (fetch-docs :kites))

   Fetch matching documents:
     (with-db db (fetch-docs :kites {:name \"blue\"}))

   Fetch only some fields (including _id):
      (with-db db (fetch-docs :kites :fields [:name :size]))

   Sort result:
     (with-db db (fetch-docs :kites {} :sort-by [:size :name]))
     (with-db db (fetch-docs :kites {} :sort-by [[:size :desc] :name]))

     Hint: :sort-by [:size] is the same as :sort-by [[:size :asc]]

   Skip and limit rows:
     Skip first row, then return 5 rows
     (with-db db (fetch-docs :kites {} :skip 1 :limit 5))

   Add a query comment shown in Mongo profiler output
     (with-db db (fetch-docs :kites {} :comment \"Look here, this is slow\"))

   Specify cursor batch size
     (with-db db (fetch-docs :kites {} :batch-size 5))

     Hint: See DBCursor.batchSize to understand negative values

   Limit query execution time
     (with-db db (fetch-docs :kites {} :max-time-millis 5000))"
  ([coll]
   (fetch-docs coll {}))
  ([coll query & {:keys [fields sort-by limit skip comment batch-size max-time-millis] :as params
                  :or {fields nil batch-size nil sort-by nil limit nil skip nil comment nil max-time-millis nil}}]
   (if-not (empty? (apply dissoc params valid-params))
     (throw (IllegalArgumentException. (str "Unsupported params: " (keys (apply dissoc params valid-params))))))
   (with-open [cursor (.find (get-collection coll)
                             (to-mongo query)
                             (to-mongo (build-field-map fields)))]
     (cond-> cursor
             sort-by (.sort (to-mongo (build-sort-map sort-by)))
             limit (.limit limit)
             skip (.skip skip)
             comment (.comment comment)
             batch-size (.batchSize batch-size)
             max-time-millis (.maxTime max-time-millis TimeUnit/MILLISECONDS))
     (map to-clojure cursor))))

(defn insert!
 "Sample:
 -------
 Insert one:
   (with-db db
     (insert! :kites {:name \"Blue\"})
     (insert! :wind {:speed 5}))

   Insert multiple:
     (with-db db (insert! :kites [{:name \"blue\"} {:name \"red\"}])"
  [coll data]
  (let [document (to-mongo data)]
    (.insert (get-collection coll)
             ^List (if (or (vector? document) (seq? document)) document (list document)))
    (to-clojure document)))

