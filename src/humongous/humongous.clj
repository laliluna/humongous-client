(ns humongous.humongous
  (:import (com.mongodb BasicDBObject DBObject DBCursor BasicDBList WriteConcern)
           (clojure.lang IPersistentVector IPersistentMap IPersistentList)
           (java.util List)
           (org.bson.types ObjectId)
           (java.util.concurrent TimeUnit))
  (:require [humongous.db :refer [write-concerns translate-write-concern]]))

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

(defn get-collection
  [db coll]
  (.getCollection db (name coll)))

(defn get-write-concern
  ([write-concern]
   (translate-write-concern write-concern))
  ([db coll write-concern]
   (if (or (nil? write-concern) (= write-concern :default-write-concern))
     (.getWriteConcern (get-collection db coll))
     (translate-write-concern write-concern))))

(defn drop!
  "Sample:
  --------
  Drop a collection:
  (drop! db :kites)"
  [db coll]
  (.drop (get-collection db coll)))

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
     (fetch-one-doc db :kites {:name \"blue\"})

   Fetch only some fields (including _id):
      (fetch-one-doc db :kites {} :fields [:name :size])

   Sort result:
     (fetch-one-doc db :kites {} :order-by [:size :name])
     (fetch-one-doc db :kites {} :order-by [[:size :desc] :name])

     Hint: :order-by [:size] is the same as :order-by [[:size :asc]]"
  [db coll query & {:keys [fields order-by] :as params
                    :or   {fields nil order-by nil}}]
  (if-not (empty? (apply dissoc params valid-params-fetch-one))
    (throw (IllegalArgumentException. (str "Unsupported params: " (keys (apply dissoc params valid-params-fetch-one))))))
  (let [mongo-coll (get-collection db coll)]
    (to-clojure
      (.findOne mongo-coll
                ^DBObject (to-mongo query)
                ^DBObject (to-mongo (build-field-map fields))
                ^DBObject (to-mongo (build-order-map order-by))
                (.getReadPreference mongo-coll)))))


(defn query
  ([db coll q]
   (query db coll q nil))
  ([db coll q fields]
   (.find (get-collection db coll)
          (to-mongo q)
          (to-mongo (build-field-map fields)))))

(defn remove-it
  "Removes documents matching the query
   Sample:
   -------
   Remove all kites with blue color
   (remove-it db :kites {:color \"blue\"})"
  ([db coll query]
   (remove-it db coll query :default-write-concern))
  ([db coll query write-concern]
   (.remove
     (get-collection db coll)
     (to-mongo query)
     (get-write-concern db coll write-concern))))

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
      (fetch-docs db :kites)

   Fetch matching documents:
     (fetch-docs db :kites {:name \"blue\"})

   Fetch only some fields (including _id):
      (fetch-docs db :kites :fields [:name :size])

   Sort result:
     (fetch-docs db :kites {} :order-by [:size :name])
     (fetch-docs db :kites {} :order-by [[:size :desc] :name])

     Hint: :order-by [:size] is the same as :order-by [[:size :asc]]

   Skip and limit rows:
     Skip first row, then return 5 rows
     (fetch-docs db :kites {} :skip 1 :limit 5)

   Add a query comment shown in Mongo profiler output
     (fetch-docs db :kites {} :query-comment \"Look here, this is slow\")

   Specify cursor batch size
     (fetch-docs db :kites {} :batch-size 5)

     Hint: See DBCursor.batchSize to understand negative values

   Limit query execution time
     (fetch-docs db :kites {} :timeout-millis 5000)

   Query index hint
     (fetch-docs db :kites {:name \"blue\"} :query-hint \"name_index\")
     (fetch-docs db :kites {:name \"blue\"} :query-hint {:name 1})"
  ([db coll]
   (fetch-docs db coll {}))
  ([db coll q & {fields      :fields order-by_ :order-by limit_ :limit skip_ :skip query-comment_ :query-comment
                 batch-size_ :batch-size timeout-millis_ :timeout-millis query-hint_ :query-hint :as params
                 :or         {fields nil batch-size nil order-by nil limit nil skip nil query-comment nil timeout-millis nil query-hint nil}}]
   (if-not (empty? (apply dissoc params valid-params))
     (throw (IllegalArgumentException. (str "Unsupported params: " (keys (apply dissoc params valid-params))))))
   (with-open [cursor (query db coll q fields)]
     (cond-> cursor
             order-by_ (order-by order-by_)
             limit_ (limit limit_)
             skip_ (skip skip_)
             query-comment_ (query-comment query-comment_)
             batch-size_ (batch-size batch-size_)
             timeout-millis_ (timeout-millis timeout-millis_)
             query-hint_ (query-hint query-hint_)
             true (fetch)))))

(defn insert!
  "Sample:
  -------
  Insert one:
    (insert! db :kites {:name \"Blue\"})

    Insert multiple:
      (insert! db :kites [{:name \"blue\"} {:name \"red\"}])

    Use a Mongo write concern
    (insert! db :kites {:name \"blue\"} :acknowledged)"
  ([db coll data]
   (insert! db coll data :default-write-concern))
  ([db coll data write-concern]
   (let [document (to-mongo data)]
     (.insert (get-collection db coll)
              ^List (if (or (vector? document) (seq? document))
                      document
                      (list document))
              (get-write-concern db coll write-concern))
     (to-clojure document))))

(defn- ensure-id [{:keys [_id]}]
  (if-not _id (throw (IllegalArgumentException. "A document update or delete requires an _id"))))

(defn update-fields!
  "Sample:
  -------
  Update the field name of a document to name 'green'
    (update-fields! db :kites {:_id 123} {:name \"green\"})

  Use a Mongo write concern
    (update-fields! db :kites {:_id 1} {:name \"green\"} :journaled)"
  ([db coll document data]
   (update-fields! db coll document data :default-write-concern))
  ([db coll document data write-concern]
   (ensure-id document)
   (.update (get-collection db coll)
            (to-mongo {:_id (:_id document)})
            (to-mongo {:$set (dissoc data :_id)})
            false
            false
            (get-write-concern db coll write-concern))))

(defn update-or-insert!
  "Sample:
  -------
  Update document if exist, else insert
    (update-or-insert! db :kites {:_id 1 :name \"blue\"})

  Use a Mongo write concern
    (update-or-insert! db :kites {:_id 1 :name \"blue\"} :journaled)"
  ([db coll document]
   (update-or-insert! db coll document :default-write-concern))
  ([db coll document write-concern]
   (ensure-id document)
   (.update (get-collection db coll)
            (to-mongo {:_id (:_id document)})
            ; id is required in document or Mongo 2.4 breaks
            (to-mongo document)
            true
            false
            (get-write-concern db coll write-concern))))

(defn update!
  "Sample:
  -------
  Update and replace an existing document
    (update! db :kites {:_id 123 :name \"blue\"})
  Use a Mongo write concern
    (update! db :kites {:_id 123 :name \"blue\"} :journaled)"
  ([db coll document]
   (update! db coll document :default-write-concern))
  ([db coll document write-concern]
   (ensure-id document)
   (.update (get-collection db coll)
            (to-mongo {:_id (:_id document)})
            (to-mongo (dissoc document :_id))
            false
            false
            (get-write-concern db coll write-concern))))

(defn remove!
  "Sample:
  -------
  Remove the given document
    (remove! db :kites {:_id 123 })
  Use a Mongo write concern
    (remove! db :kites {:_id 123 } :journaled)"
  ([db coll document]
   (remove! db coll document :default-write-concern))
  ([db coll document write-concern]
   (ensure-id document)
   (.remove (get-collection db coll)
            (to-mongo {:_id (:_id document)})
            (get-write-concern db coll write-concern))))

(defn ensure-version [{:keys [_version]}]
  (if-not _version (throw (IllegalArgumentException. "_version field is required for optimistic locking"))))

(defn update-optimistic-lock!
  "Used internally by (optimistic ...)"
  ([db coll old-document data]
   (update-optimistic-lock! db coll old-document data :default-write-concern))
  ([db coll {:keys [_id _version] :as old-document} data write-concern]
   (ensure-id old-document)
   (ensure-version old-document)
   (let [r (.update (get-collection db coll)
                    (to-mongo {:_id _id :_version _version})
                    (to-mongo {:$set (assoc (dissoc data :_id) :_version (inc _version))})
                    false
                    false
                    (get-write-concern db coll write-concern))]
     (if (= 0 (.getN r))
       false
       r))))

(defn remove-optimistic-lock!
  "Used internally by (optimistic ...)"
  ([db coll document]
   (remove-optimistic-lock! db coll document :default-write-concern))
  ([db coll {:keys [_id _version] :as document} write-concern]
   (ensure-id document)
   (ensure-version document)
   (let [r (.remove (get-collection db coll)
                  (to-mongo {:_id _id :_version _version})
                  (get-write-concern db coll write-concern))]
     (if (= 0 (.getN r))
       false
       r))))

(defmacro optimistic [[fn & args]]
  `(cond (= update-fields! ~fn)
         (update-optimistic-lock! ~@args)
         (= update! ~fn)
         (update-optimistic-lock! ~(first args)
                                ~(second args)
                                ~(nth args 2)
                                ~(nth args 2)
                                ~@(drop 3 args))
         (= remove! ~fn)
         (remove-optimistic-lock! ~@args)
         :else (throw (IllegalArgumentException. (str "Optimistic lock is not supported for fn: " #'~fn)))))

(defn create-index
  "Sample:
  --------
  (create-index db :kites {:name 1})
  (create-index db :kites {:name \"text\"}  {:default_language \"en\"})"
  ([db coll data]
   (.createIndex (get-collection db coll) (to-mongo data)))
  ([db coll data options]
   (.createIndex (get-collection db coll) (to-mongo data) (to-mongo options))))

(defn get-indexes
  "Sample:
  --------
  (get-indexes db :kites"
  [db coll]
  (.getIndexInfo (get-collection db coll)))

(defn drop-index
  "Sample:
  --------
  (drop-index db :kites \"name_text\")"
  [db coll name]
  (.dropIndex (get-collection db coll) name))
