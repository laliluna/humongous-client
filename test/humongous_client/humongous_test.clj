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
    (fact "Can insert and fetch complex documents"
          (with-db db
                   (insert! :kites {:_id 1 :name "Blue" :types {:size [1 2 3 4] :color "Blue" :price nil}})
                   (fetch-docs :kites {} :sort-by [:_id]) => [{:_id 1 :name "Blue" :types {:size [1 2 3 4] :color "Blue" :price nil}}]))
    (fact "Can insert multiple document"
          (with-db db (insert! :kites [{:_id 1 :name "blue"} {:_id 2 :name "red"}])
                   (fetch-docs :kites) => [{:_id 1 :name "blue"} {:_id 2 :name "red"}])))

  (facts "Fetching docs"
         (against-background (before :facts (with-db db (drop! :kites))))
         (fact "Fetch matching documents"
               (with-db db
                        (insert! :kites {:_id 1 :name "blue"})
                        (insert! :kites {:_id 2 :name "red"})
                        (fetch-docs :kites {:name "red"}) => [{:_id 2 :name "red"}]))
         (fact "Use Mongo query API"
               (with-db db
                        (insert! :kites {:_id 1 :name "blue" :age 11})
                        (insert! :kites {:_id 2 :name "red" :age 22})
                        (fetch-docs :kites {:age {:$gt 12}}) => [{:_id 2 :name "red" :age 22}]))
         (fact "Select fields of document"
               (with-db db
                        (insert! :kites {:_id 1 :name "blue" :size 11 :shape "Delta"})
                        (fetch-docs :kites {} :fields [:name :shape]) => [{:_id 1 :name "blue" :shape "Delta"}]))
         (fact "Order documents by fields"
               (with-db db
                        (insert! :kites [{:_id 1 :name "blue"} {:_id 2 :name "amber"} {:_id 3 :name "red"}])
                        (fetch-docs :kites {} :sort-by [[:name :asc]]) =>
                        [{:_id 2 :name "amber"} {:_id 1 :name "blue"} {:_id 3 :name "red"}]
                        (fetch-docs :kites {} :sort-by [[:name :desc]]) =>
                        [{:_id 3 :name "red"} {:_id 1 :name "blue"}{:_id 2 :name "amber"}]))
         (fact "Order documents by multiple fields"
               (with-db db
                        (insert! :kites [{:_id 1 :name "blue" :size 5} {:_id 2 :name "amber" :size 7} {:_id 3 :name "blue" :size 11}])
                        (fetch-docs :kites {} :sort-by [:name :size]) =>
                        [{:_id 2 :name "amber" :size 7} {:_id 1 :name "blue" :size 5} {:_id 3 :name "blue" :size 11}]
                        (fetch-docs :kites {} :sort-by [:name [:size :desc]]) =>
                        [{:_id 2 :name "amber" :size 7} {:_id 3 :name "blue" :size 11} {:_id 1 :name "blue" :size 5} ]))))

