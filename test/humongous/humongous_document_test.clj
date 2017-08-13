(ns humongous.humongous-document-test
  (:import (com.mongodb BasicDBObject)
           (org.bson.types ObjectId))
  (:require [midje.sweet :refer :all]
            [humongous.humongous :refer :all]
            [humongous.db :as mongodb]
            [humongous.test-util :refer [get-db-uri]]))


(mongodb/with-open!
  [db (mongodb/create-db-client (get-db-uri "test"))]
  (facts "Document API"
         (against-background (before :facts (drop! db :kites)))
         (fact "Can insert document"
               (let [v (insert! db :kites {:name "Blue"})]
                 v => (contains {:name "Blue"})
                 (:_id v) => truthy))

         (fact "Can fetch inserted documents"
               (insert! db :kites {:_id 1 :name "Blue"})
               (fetch-docs db :kites) => [{:_id 1 :name "Blue"}])
         (fact "Can insert complex documents"
               (insert! db :kites {:_id 1 :name "Blue" :types {:size [1 2 3 4] :color "Blue" :price nil}})
               (fetch-docs db :kites {} :order-by [:_id]) => [{:_id 1 :name "Blue" :types {:size [1 2 3 4] :color "Blue" :price nil}}])
         (fact "Can insert multiple document"
               (insert! db :kites [{:_id 1 :name "blue"} {:_id 2 :name "red"}])
               (fetch-docs db :kites) => [{:_id 1 :name "blue"} {:_id 2 :name "red"}])
         (fact "Use a write concern, when inserting"
               (insert! db :kites {:name "blue"} :acknowledged) => truthy)
         (fact "Update fields in document"
               (insert! db :kites {:_id 1 :name "blue" :size 5})
               (update-fields! db :kites {:_id 1} {:name "green"})
               (fetch-first-doc db :kites {:_id 1}) => {:_id 1 :name "green" :size 5})
         (fact "Reject to update or remove without _id"
               (update-fields! db :kites {:foo "abc"} {:name "green"}) => (throws IllegalArgumentException)
               (update! db :kites {:foo "abc"}) => (throws IllegalArgumentException)
               (remove! db :kites {:name "green"}) => (throws IllegalArgumentException))
         (fact "Update document"
               (insert! db :kites [{:_id 1 :name "blue" :field-to-lose-after-update 5}])
               (update! db :kites {:_id 1 :name "green"})
               (fetch-first-doc db :kites {:name "green"}) => {:_id 1 :name "green"})
         (fact "Update or insert document"
               (insert! db :kites {:_id 1 :name "blue" :size 5})
               (update-or-insert! db :kites {:_id 2 :name "red"})
               (fetch-first-doc db :kites {:name "red"}) => {:_id 2 :name "red"}
               (update-or-insert! db :kites {:_id 1 :name "green"})
               (fetch-first-doc db :kites {:name "green"}) => {:_id 1 :name "green"})
         (fact "Remove document"
               (let [kite (insert! db :kites {:name "blue"})]
                 (remove! db :kites kite)
                 (count (fetch-docs db :kites)) => 0))
         (fact "Use write concerns on update and remove"
               (insert! db :kites {:_id 1 :name "blue" :size 5})
               (update-fields! db :kites {:_id 1} {:name "green"} :journaled) => truthy
               (update! db :kites {:_id 1 :name "red"} :unacknowledged)
               (remove! db :kites {:_id 1} :acknowledged))
         (fact "Fail on wrong write concern"
               (update! db :kites {:_id 1 :name "red"} :bad-write-concern) => (throws IllegalArgumentException)))
  (facts "Optimistic locking with the document API"
         (against-background
           (before :facts (drop! db :kites)))
         (fact "In case of optimistic locking, compare _id and _version and return false, if update fails"
               (optimistic (update-fields! db :kites {:_id 1 :_version 1} {:name "green"})) => falsey)
         (fact "Update version if success"
               (insert! db :kites {:_id 1 :name "blue" :_version 1})
               (optimistic (update-fields! db :kites {:_id 1 :_version 1} {:name "green"})) => truthy
               (fetch-first-doc db :kites {}) => {:_id 1 :_version 2 :name "green"})
         (fact "Require _version field"
               (optimistic (update-fields! db :kites {:_id 1} {})) => (throws IllegalArgumentException))
         (fact "Optimistic locking is supported for update! and remove!"
               (optimistic (update! db :kites {:_id 1 :_version 1})) => falsey
               (insert! db :kites {:_id 1 :name "blue" :_version 1})
               (optimistic (update! db :kites {:_id 1 :_version 1})) => truthy
               (optimistic (remove! db :kites {:_id 1 :_version 1})) => falsey
               (optimistic (remove! db :kites {:_id 1 :_version 2})) => truthy))
  (facts "Fetching docs"
         (against-background
           (before :facts (do (drop! db :kites)
                              (insert! db :kites [{:_id 1 :name "blue" :size 5}
                                                  {:_id 2 :name "amber" :size 7}
                                                  {:_id 3 :name "red" :size 7}]))))
         (fact "Fetch matching documents"
               (fetch-docs db :kites {:name "red"}) => [{:_id 3 :name "red" :size 7}])
         (fact "Use Mongo query API"
               (fetch-docs db :kites {:size {:$lt 7}}) => [{:_id 1 :name "blue" :size 5}])
         (fact "Select fields of document"
               (first (fetch-docs db :kites {} :fields [:size] :order-by [:_id])) => {:_id 1 :size 5})
         (fact "Order documents by fields"
               (fetch-docs db :kites {} :fields [:name] :order-by [[:name :asc]]) =>
               [{:_id 2 :name "amber"} {:_id 1 :name "blue"} {:_id 3 :name "red"}]
               (fetch-docs db :kites {} :fields [:name] :order-by [[:name :desc]]) =>
               [{:_id 3 :name "red"} {:_id 1 :name "blue"} {:_id 2 :name "amber"}])
         (fact "Order documents by multiple fields"
               (fetch-docs db :kites {} :order-by [:size :name]) =>
               [{:_id 1 :name "blue" :size 5} {:_id 2 :name "amber" :size 7} {:_id 3 :name "red" :size 7}]
               (fetch-docs db :kites {} :order-by [[:size :desc] :name]) =>
               [{:_id 2 :name "amber" :size 7} {:_id 3 :name "red" :size 7} {:_id 1 :name "blue" :size 5}])
         (fact "Limit number of returned rows"
               (count (fetch-docs db :kites {} :limit 2)) => 2)
         (fact "Skips rows of query result"
               (fetch-docs db :kites {} :skip 1 :order-by [:_id] :fields [:_id]) => [{:_id 2} {:_id 3}])
         (fact "Can add a comment to profiler output (look at the profiler output to validate)"
               (fetch-docs db :kites {} :query-comment "Hello world"))
         (fact "Define batch size of cursor"
               (fetch-docs db :kites {} :batch-size 5))
         (fact "Limit query execution time"
               (fetch-docs db :kites {} :timeout-millis 5000))
         (fact "Give a query hint to use an index"
               (create-index db :kites {:name 1})
               (fetch-docs db :kites {:name "blue"} :query-hint "name_1")
               (fetch-docs db :kites {:name "blue"} :query-hint {:name 1}))
         (fact "Can create and drop index"
           (count (get-indexes db :kites)) => 1
           (create-index db :kites {:name 1})
           (count (get-indexes db :kites)) => 2
           (drop-index db :kites "name_1")
           (count (get-indexes db :kites)) => 1)
         (fact "Can create index with options"
           (create-index db :kites {:name "text"}  {:default_language "en"}))

         (fact "Chained API to allow full access"
               (create-index db :kites {:name 1})
               (->
                 (query db :kites {:size 7})
                 (order-by [:name])
                 (skip 1)
                 (limit 1)
                 (query-comment "This query might be slow")
                 (batch-size 10)
                 (timeout-millis 2000)
                 (query-hint {:name 1})
                 (fetch)) => [{:_id 3 :name "red" :size 7}]
               (->
                 (query db :kites {:size 7})
                 (explain)) => truthy
               (->
                 (query db :kites {:size 7})
                 (count-rows)) => 2)))
