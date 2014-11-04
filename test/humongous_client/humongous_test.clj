(ns humongous-client.humongous-test
  (:import (com.mongodb BasicDBObject)
           (org.bson.types ObjectId))
  (:require [midje.sweet :refer :all]
            [humongous-client.humongous :refer :all]
            [humongous-client.db :as mongodb]))

(defmacro with-open! [bindings & body]
  `(let ~bindings
     (try
       ~@body
       (finally
         (~'mongodb/close! ~(bindings 0))))))

(with-open!
  [db (mongodb/create-db-client "mongodb://localhost:27017/test")]
  (facts
    (against-background (before :facts (with-db db (drop! :kites))))
    (fact "Can insert document"
          (let [v (with-db db (insert! :kites {:name "Blue"}))]
            v => (contains {:name "Blue"})
            (:_id v) => truthy))

    (fact "Can fetch inserted documents"
          (with-db db
                   (insert! :kites {:_id 1 :name "Blue"})
                   (fetch-docs :kites) => [{:_id 1 :name "Blue"}]))
    (fact "Can insert complex documents"
          (with-db db
                   (insert! :kites {:_id 1 :name "Blue" :types {:size [1 2 3 4] :color "Blue" :price nil}})
                   (fetch-docs :kites {} :order-by [:_id]) => [{:_id 1 :name "Blue" :types {:size [1 2 3 4] :color "Blue" :price nil}}]))
    (fact "Can insert multiple document"
          (with-db db (insert! :kites [{:_id 1 :name "blue"} {:_id 2 :name "red"}])
                   (fetch-docs :kites) => [{:_id 1 :name "blue"} {:_id 2 :name "red"}]))
    (fact "Update fields in document"
          (with-db db
                   (insert! :kites {:_id 1 :name "blue" :size 5})
                   (update! :kites {:name "blue"} {:name "green"})
                   (fetch-first-doc :kites {:_id 1}) => {:_id 1 :name "green" :size 5}))
    (fact "Update first document"
          (with-db db
                   (insert! :kites [{:_id 1 :name "blue" :size 5} {:_id 2 :name "blue" :size 7}])
                   (update-first! :kites {:name "blue"} {:name "green"})
                   (count (fetch-docs :kites {:name "green"})) => 1))
    (fact "Replace first document"
          (with-db db
                   (insert! :kites [{:_id 1 :name "blue" :size 5} {:_id 2 :name "blue" :size 5}])
                   (replace-first! :kites {:name "blue"} {:name "green"})
                   (count (fetch-docs :kites {:name "green"})) => 1
                   (:size (fetch-first-doc :kites {:name "green"})) => falsey))
    (fact "Update or insert document"
          (with-db db
                   (insert! :kites {:_id 1 :name "blue" :size 5})
                   (update-or-insert! :kites {:name "red"} {:name "red"})
                   (count (fetch-docs :kites {:name "red"})) => 1
                   (update-or-insert! :kites {:name "blue"} {:name "green"})
                   (fetch-first-doc :kites {:name "green"}) => {:_id 1 :name "green" :size 5}))
    (fact "Remove matching documents"
          (with-db db
                   (let [kite (insert! :kites {:name "blue"})]
                     (remove! :kites kite)
                     (count (fetch-docs :kites)) => 0))))

  (facts "Fetching docs"
         (against-background
           (before :facts (with-db db (drop! :kites)
                                   (insert! :kites [{:_id 1 :name "blue" :size 5}
                                                    {:_id 2 :name "amber" :size 7}
                                                    {:_id 3 :name "red" :size 7}]))))
         (fact "Fetch matching documents"
               (with-db db
                        (fetch-docs :kites {:name "red"}) => [{:_id 3 :name "red" :size 7}]))
         (fact "Use Mongo query API"
               (with-db db
                        (fetch-docs :kites {:size {:$lt 7}}) => [{:_id 1 :name "blue" :size 5}]))
         (fact "Select fields of document"
               (with-db db
                        (first (fetch-docs :kites {} :fields [:size] :order-by [:_id])) => {:_id 1 :size 5}))
         (fact "Order documents by fields"
               (with-db db
                        (fetch-docs :kites {} :fields [:name] :order-by [[:name :asc]]) =>
                        [{:_id 2 :name "amber"} {:_id 1 :name "blue"} {:_id 3 :name "red"}]
                        (fetch-docs :kites {} :fields [:name] :order-by [[:name :desc]]) =>
                        [{:_id 3 :name "red"} {:_id 1 :name "blue"}{:_id 2 :name "amber"}]))
         (fact "Order documents by multiple fields"
               (with-db db
                        (fetch-docs :kites {} :order-by [:size :name]) =>
                        [{:_id 1 :name "blue" :size 5} {:_id 2 :name "amber" :size 7}  {:_id 3 :name "red" :size 7}]
                        (fetch-docs :kites {} :order-by [[:size :desc] :name]) =>
                        [{:_id 2 :name "amber" :size 7} {:_id 3 :name "red" :size 7} {:_id 1 :name "blue" :size 5} ]))
         (fact "Limit number of returned rows"
               (count (with-db db (fetch-docs :kites {} :limit 2))) => 2)
         (fact "Skips rows of query result"
               (with-db db (fetch-docs :kites {} :skip 1 :order-by [:_id] :fields [:_id])) => [{:_id 2} {:_id 3}])
         (fact "Can add a comment to profiler output (look at the profiler output to validate)"
               (with-db db (fetch-docs :kites {} :query-comment "Hello world")))
         (fact "Define batch size of cursor"
               (with-db db (fetch-docs :kites {} :batch-size 5)))
         (fact "Limit query execution time"
               (with-db db (fetch-docs :kites {} :timeout-millis 5000)))
         (fact "Give a query hint to use an index"
               (with-db db
                        (ensure-index :kites {:name 1})
                        (fetch-docs :kites {:name "blue"} :query-hint "name_1")
                        (fetch-docs :kites {:name "blue"} :query-hint {:name 1})))
         (fact "Chained API to allow full access"
               (with-db db
                        (ensure-index :kites {:name 1})
                        (->
                          (query :kites {:size 7})
                          (order-by [:name])
                          (skip 1)
                          (limit 1)
                          (query-comment "This query might be slow")
                          (batch-size 10)
                          (timeout-millis 2000)
                          (query-hint {:name 1})
                          (fetch)) => [{:_id 3 :name "red" :size 7}]
                        (->
                          (query :kites {:size 7})
                          (explain)) => truthy
                        (->
                          (query :kites {:size 7})
                          (count-rows)) => 2)
               )))
