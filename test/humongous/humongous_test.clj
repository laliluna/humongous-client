(ns humongous.humongous-test
  (:import (com.mongodb BasicDBObject)
           (org.bson.types ObjectId))
  (:require [midje.sweet :refer :all]
            [humongous.humongous :refer :all]
            [humongous.db :as mongodb]
            [humongous.test-util :refer [get-db-uri]]))


(mongodb/with-open!
  [db (mongodb/create-db-client (get-db-uri "test"))]
  (facts "Interact with collection"
    (against-background (before :facts (drop! db :kites)))
    (fact "Remove documents"
      (insert! db :kites {:name "blue"})
      (insert! db :kites {:name "red"})
      (remove-it db :kites {:name "blue"})
      (let [docs (fetch-docs db :kites)]
        (count docs) => 1
        (first docs) => (contains {:name "red"})))
    (fact "Remove documents supports write concerns"
      (remove-it db :kites {:name "blue"} :journaled))))
