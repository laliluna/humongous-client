(ns humongous.humongous-coercion-test
  (:import (com.mongodb BasicDBObject)
           (org.bson.types ObjectId))
  (:require [midje.sweet :refer :all]
            [humongous.humongous :refer :all]))

(fact "Convert map to a Mongo"
      (let [v (to-mongo {:foo "abc" :bar 1})]
        (.get v "foo") => "abc"
        (.get v "bar") => 1))

(fact "Convert nested map to a Mongo"
      (let [v (to-mongo {:foo {:bar 2}})]
        (type (.get v "foo")) => BasicDBObject
        (.get (.get v "foo") "bar") => 2))

(fact "Convert vector of map to Mongo"
      (let [v (to-mongo {:foo [{:bar 1} {:bar 2}]})]
        (.get (first (.get v "foo")) "bar") => 1
        (.get (second (.get v "foo")) "bar") => 2))

(fact "Convert vector of map to Mongo"
      (let [v (to-mongo {:foo '({:bar 1} {:bar 2})})]
        (.get (first (.get v "foo")) "bar") => 1
        (.get (second (.get v "foo")) "bar") => 2))

(fact "Convert nil to a Mongo object"
      (let [v (to-mongo {:foo nil})]
        (.get v "foo") => nil))

(fact "Convert from Mongo document to map"
      (to-clojure (.append (BasicDBObject. "name" "blue") "age" 5)) => (contains {:name "blue" :age 5}))

(fact "Convert from vector of Mongo document to vector of maps"
      (to-clojure [(.append (BasicDBObject. "name" "blue") "age" 5)]) => [{:name "blue" :age 5}])

(fact "Convert nested objects back to nested maps"
      (to-clojure (to-mongo {:foo {:bar {:size [1 2 3] :name "bazz"}}})) => {:foo {:bar {:size [1 2 3] :name "bazz"}}})

(fact "Convert BasicDbObject with nil value"
      (to-clojure (BasicDBObject. "name" nil)) => {:name nil})

(fact "Convert ObjectId to string representation"
      (to-clojure (ObjectId. "012345678901234567890123")) => "012345678901234567890123")
