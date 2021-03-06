(ns humongous.command
  (:import (com.mongodb DBObject DBCollection WriteConcern BasicDBObject))
  (:require [humongous.humongous :as h]))


(defn unordered-bulk [db coll]
  (.initializeUnorderedBulkOperation (h/get-collection db coll)))

(defn ordered-bulk [db coll]
  (.initializeOrderedBulkOperation (h/get-collection db coll)))

(defn insert [bulk data & moredata]
  (doseq [v (flatten [data (or moredata [])])]
    (.insert bulk ^DBObject (h/to-mongo v)))
  bulk)

(defn find-doc [bulk query]
  (.find bulk
         ^DBObject (h/to-mongo query)))

(defn- to-update-request [data]
  (BasicDBObject. "$set"
                  ^DBObject (h/to-mongo data)))

(defn update [bulk query data]
  (.update
    (find-doc bulk query)
    (to-update-request data))
  bulk)

(defn update-or-insert [bulk query data]
  (.update
    (.upsert (find-doc bulk query))
    (to-update-request data))
  bulk)

(defn update-first [bulk query data]
  (.updateOne
    (find-doc bulk query)
    (to-update-request data))
  bulk)

(defn update-first-or-insert [bulk query data]
  (.updateOne
    (.upsert (find-doc bulk query))
    (to-update-request data))
  bulk)

(defn replace-first [bulk query data]
  (.replaceOne
    (find-doc bulk query)
    ^DBObject (h/to-mongo data))
  bulk)

(defn replace-first-or-insert [bulk query data]
  (.replaceOne
    (.upsert (find-doc bulk query))
    ^DBObject (h/to-mongo data))
  bulk)

(defn execute!
  ([bulk]
   (.execute bulk))
  ([bulk write-concern]
   (.execute bulk (h/get-write-concern write-concern))))

(defn remove-doc [bulk query]
  (.remove
    (find-doc bulk query))
  bulk)

(defn remove-first-doc [bulk query]
  (.removeOne
    (find-doc bulk query))
  bulk)




