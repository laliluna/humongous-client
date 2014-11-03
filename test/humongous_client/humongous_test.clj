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

(with-open! [db (mongodb/create-db-client "mongodb://localhost:27017/test")]
  (facts
    (against-background (before :facts (with-db db (drop! :kites))))
    (fact "Can insert document"
          (let [ v (with-db db (insert! :kites {:name "Blue"}))]
            v => (contains {:name "Blue"})
            (:_id v) => truthy))

    (fact "Can fetch inserted documents"
          (with-db db
                   (insert! :kites {:_id 1 :name "Blue"})
                   (fetch-docs :kites) => [{:_id 1 :name "Blue"}]))
    (fact "Can insert and fetch complex documents"
          (with-db db
                   (insert! :kites {:_id 1 :name "Blue" :types {:size [1 2 3 4] :color "Blue" :price nil}})
                   (fetch-docs :kites) => [{:_id 1 :name "Blue" :types {:size [1 2 3 4] :color "Blue" :price nil}}]))))
