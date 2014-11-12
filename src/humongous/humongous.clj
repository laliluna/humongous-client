(ns humongous.humongous
  (:import (com.mongodb BasicDBObject DBObject DBCursor BasicDBList WriteConcern)
           (clojure.lang IPersistentVector IPersistentMap IPersistentList)
           (java.util List)
           (org.bson.types ObjectId)
           (java.util.concurrent TimeUnit))
  (:require [humongous.db :refer [write-concerns translate-write-concern]]))

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

  BasicDBList
  (to-clojure [v]
    (map to-clojure v))

  IPersistentVector
  (to-clojure [v]
    (map to-clojure v))

  IPersistentList
  (to-clojure [v]
    (map to-clojure v))

  nil
  (to-clojure [v]
    v)

  Object
  (to-clojure [v] v))

(defn get-collection [coll]
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

(defn- build-order-map [m]
  (if m
    (apply array-map
           (flatten (map (fn [v]
                           (if (vector? v)
                             [(first v)
                              (if (= (second v) :asc) 1 -1)]
                             [v 1])) m)))))


(def valid-params-fetch-one [:fields :order-by])

(defn fetch-first-doc
  "Sample:
   --------
   Fetch first matching document:
     (with-db db (fetch-one-doc :kites {:name \"blue\"}))

   Fetch only some fields (including _id):
      (with-db db (fetch-one-doc :kites {} :fields [:name :size]))

   Sort result:
     (with-db db (fetch-one-doc :kites {} :order-by [:size :name]))
     (with-db db (fetch-one-doc :kites {} :order-by [[:size :desc] :name]))

     Hint: :order-by [:size] is the same as :order-by [[:size :asc]]"
  [coll query & {:keys [fields order-by] :as params
                 :or   {fields nil order-by nil}}]
  (if-not (empty? (apply dissoc params valid-params-fetch-one))
    (throw (IllegalArgumentException. (str "Unsupported params: " (keys (apply dissoc params valid-params-fetch-one))))))
  (let [mongo-coll (get-collection coll)]
    (to-clojure
      (.findOne mongo-coll
                ^DBObject (to-mongo query)
                ^DBObject (to-mongo (build-field-map fields))
                ^DBObject (to-mongo (build-order-map order-by))
                (.getReadPreference mongo-coll)
                ))))

(defn query
  ([coll q]
   (query coll q nil))
  ([coll q fields]
   (.find (get-collection coll)
          (to-mongo q)
          (to-mongo (build-field-map fields)))))

(defn fetch [^DBCursor cursor]
  (doall (map to-clojure cursor)))

(defn explain [^DBCursor cursor]
  (to-clojure (.explain cursor)))

(defn count-rows [^DBCursor cursor]
  (.count cursor))

(defn order-by [^DBCursor cursor order]
  (.sort cursor (to-mongo (build-order-map order))))

(defn limit [^DBCursor cursor rows]
  (.limit cursor rows))

(defn query-comment [^DBCursor cursor c]
  (.comment cursor c))

(defn batch-size [^DBCursor cursor size]
  (.batchSize cursor size))

(defn timeout-millis [^DBCursor cursor millis]
  (.maxTime cursor millis TimeUnit/MILLISECONDS))

(defn query-hint [^DBCursor cursor index-hint]
  (.hint cursor (to-mongo index-hint)))

(defn skip [^DBCursor cursor rows]
  (.skip cursor rows))

(def valid-params [:fields :order-by :limit :skip :query-comment :batch-size :timeout-millis :query-hint])

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
     (with-db db (fetch-docs :kites {} :order-by [:size :name]))
     (with-db db (fetch-docs :kites {} :order-by [[:size :desc] :name]))

     Hint: :order-by [:size] is the same as :order-by [[:size :asc]]

   Skip and limit rows:
     Skip first row, then return 5 rows
     (with-db db (fetch-docs :kites {} :skip 1 :limit 5))

   Add a query comment shown in Mongo profiler output
     (with-db db (fetch-docs :kites {} :query-comment \"Look here, this is slow\"))

   Specify cursor batch size
     (with-db db (fetch-docs :kites {} :batch-size 5))

     Hint: See DBCursor.batchSize to understand negative values

   Limit query execution time
     (with-db db (fetch-docs :kites {} :timeout-millis 5000))

   Query index hint
     (with-db db
       (fetch-docs :kites {:name \"blue\"} :query-hint \"name_index\")
       (fetch-docs :kites {:name \"blue\"} :query-hint {:name 1}))"
  ([coll]
   (fetch-docs coll {}))
  ([coll q & {fields      :fields order-by_ :order-by limit_ :limit skip_ :skip query-comment_ :query-comment
              batch-size_ :batch-size timeout-millis_ :timeout-millis query-hint_ :query-hint :as params
              :or         {fields nil batch-size nil order-by nil limit nil skip nil query-comment nil timeout-millis nil query-hint nil}}]
   (if-not (empty? (apply dissoc params valid-params))
     (throw (IllegalArgumentException. (str "Unsupported params: " (keys (apply dissoc params valid-params))))))
   (with-open [cursor (query coll q fields)]
     (cond-> cursor
             order-by_ (order-by order-by_)
             limit_ (limit limit_)
             skip_ (skip skip_)
             query-comment_ (query-comment query-comment_)
             batch-size_ (batch-size batch-size_)
             timeout-millis_ (timeout-millis timeout-millis_)
             query-hint_ (query-hint query-hint_)
             true (fetch)))))



(defn get-write-concern
  ([write-concern]
   (translate-write-concern write-concern))
  ([coll write-concern]
   (if (or (nil? write-concern) (= write-concern :default-write-concern))
     (.getWriteConcern (get-collection coll))
     (translate-write-concern write-concern))))

(defn insert!
  "Sample:
  -------
  Insert one:
    (with-db db
      (insert! :kites {:name \"Blue\"})
      (insert! :wind {:speed 5}))

    Insert multiple:
      (with-db db (insert! :kites [{:name \"blue\"} {:name \"red\"}])

    Use a Mongo write concern
    (with-db db (insert! :kites {:name \"blue\"} :acknowledged))"
  ([coll data]
   (insert! coll data :default-write-concern))
  ([coll data write-concern]
   (let [document (to-mongo data)]
     (.insert (get-collection coll)
              ^List (if (or (vector? document) (seq? document))
                      document
                      (list document))
              (get-write-concern coll write-concern))
     (to-clojure document))))

(defn- ensure-id [{:keys [_id]}]
  (if-not _id (throw (IllegalArgumentException. "A document update requires an _id"))))

(defn update-fields!
  "Sample:
  -------
  Update the field name of a document to name 'green'
    (with-db db (update-fields! :kites {:_id 123} {:name \"green\"}))

  Use a Mongo write concern
    (update-fields! :kites {:_id 1} {:name \"green\"} :journaled)"
  ([coll document data]
   (update-fields! coll document data :default-write-concern))
  ([coll document data write-concern]
   (ensure-id document)
   (.update (get-collection coll)
            (to-mongo {:_id (:_id document)})
            (to-mongo {:$set (dissoc data :_id)})
            false
            false
            (get-write-concern coll write-concern))))

(defn update-or-insert!
  "Sample:
  -------
  Update document if exist, else insert
    (with-db db (update-or-insert! :kites {:_id 1 :name \"blue\"}))

  Use a Mongo write concern
    (with-db db (update-or-insert! :kites {:_id 1 :name \"blue\"} :journaled))"
  ([coll document]
   (update-or-insert! coll document :default-write-concern))
  ([coll document write-concern]
   (ensure-id document)
   (.update (get-collection coll)
            (to-mongo {:_id (:_id document)})
            ; id is required in document or Mongo 2.4 breaks
            (to-mongo document)
            true
            false
            (get-write-concern coll write-concern))))

(defn update!
  "Sample:
  -------
  Update and replace an existing document
    (with-db db (update! :kites {:_id 123 :name \"blue\"}))
  Use a Mongo write concern
    (with-db db (update! :kites {:_id 123 :name \"blue\"} :journaled))"
  ([coll document]
   (update! coll document :default-write-concern))
  ([coll document write-concern]
   (ensure-id document)
   (.update (get-collection coll)
            (to-mongo {:_id (:_id document)})
            (to-mongo (dissoc document :_id))
            false
            false
            (get-write-concern coll write-concern))))

(defn remove!
  "Sample:
  -------
  Remove all documents with name 'blue'
    (with-db db (remove! :kites {:_id 123 }))
  Use a Mongo write concern
    (with-db db (remove! :kites {:_id 123 } :journaled))"
  ([coll document]
   (remove! coll document :default-write-concern))
  ([coll document write-concern]
   (ensure-id document)
   (.remove (get-collection coll)
            (to-mongo {:_id (:_id document)})
            (get-write-concern coll write-concern))))

(defn ensure-version [{:keys [_version]}]
  (if-not _version (throw (IllegalArgumentException. "_version field is required for optimistic locking"))))

(defn update-optimistic-lock!
  "Used internally by (optimistic ...)"
  ([coll old-document data]
   (update-optimistic-lock! coll old-document data :default-write-concern))
  ([coll {:keys [_id _version] :as old-document} data write-concern]
   (ensure-id old-document)
   (ensure-version old-document)
   (let [r (.update (get-collection coll)
                    (to-mongo {:_id _id :_version _version})
                    (to-mongo {:$set (assoc (dissoc data :_id) :_version (inc _version))})
                    false
                    false
                    (get-write-concern coll write-concern))]
     (if (= 0 (.getN r))
       false
       r))))

(defn remove-optimistic-lock!
  "Used internally by (optimistic ...)"
  ([coll document]
   (remove-optimistic-lock! coll document :default-write-concern))
  ([coll {:keys [_id _version] :as document} write-concern]
   (ensure-id document)
   (ensure-version document)
   (let [r (.remove (get-collection coll)
                  (to-mongo {:_id _id :_version _version})
                  (get-write-concern coll write-concern))]
     (if (= 0 (.getN r))
       false
       r))))

(defmacro optimistic [[fn & args]]
  `(cond (= update-fields! ~fn)
         (update-optimistic-lock! ~@args)
         (= update! ~fn)
         (update-optimistic-lock! ~(first args)
                                ~(second args)
                                ~(second args)
                                ~@(drop 2 args))
         (= remove! ~fn)
         (remove-optimistic-lock! ~@args)
         :else (throw (IllegalArgumentException. (str "Optimistic lock is not supported for fn: " #'~fn)))))

(defn ensure-index
  "Sample:
  --------
  (with-db db (ensure-index :kites {:name 1}))"
  [coll data]
  (.ensureIndex (get-collection coll) (to-mongo data)))