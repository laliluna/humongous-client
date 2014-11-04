(ns humongous-client.bulk
  (:import (com.mongodb DBObject DBCollection WriteConcern BasicDBObject))
  (:require [humongous-client.humongous :as h]))


(defn unordered-bulk [coll]
  (.initializeUnorderedBulkOperation (h/get-collection coll)))

(defn ordered-bulk [coll]
  (.initializeOrderedBulkOperation (h/get-collection coll)))

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
  ;([bulk write-concern]  (.execute bulk (write-concern m/write-concern-map))))
  )
(defn remove-doc [bulk query]
  (.remove
    (find-doc bulk query))
  bulk)

(defn remove-one-doc [bulk query]
  (.removeOne
    (find-doc bulk query))
  bulk)




