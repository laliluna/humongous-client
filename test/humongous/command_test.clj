(ns humongous.command-test
  (:require [humongous.command :refer [unordered-bulk ordered-bulk insert
                                       update update-first replace-first
                                       update-or-insert update-first-or-insert replace-first-or-insert
                                       remove-doc remove-first-doc
                                       execute!]]
            [midje.sweet :refer :all]
            [humongous.humongous :as h]
            [humongous.db :as mongodb]
            [humongous.test-util :refer [get-db-uri]]))

(mongodb/with-open!
  [db (mongodb/create-db-client (get-db-uri "test"))]

  (facts "Bulk insert operations" (against-background
                                    (before :facts (h/drop! db :dummy)))
         (fact "Can execute one insert"
               (-> (unordered-bulk db :dummy)
                   (insert {:_id 1 :foo 2})
                   (execute!))
               (first (h/fetch-docs db :dummy {})) => {:_id 1 :foo 2})
         (fact "Can execute multiple insert"
               (-> (unordered-bulk db :dummy)
                   (insert {:_id 1 :foo "a"})
                   (insert {:_id 2 :foo "b"})
                   (execute!))
               (h/fetch-docs db :dummy {}) =>
               '({:_id 1 :foo "a"} {:_id 2 :foo "b"}))
         (fact "Can insert collection"
               (-> (unordered-bulk db :dummy)
                   (insert [{:_id 1 :foo "a"}, {:_id 2 :foo "b"}])
                   (execute!))
               (h/fetch-docs db :dummy {}) =>
               '({:_id 1 :foo "a"} {:_id 2 :foo "b"}))
         (fact "Can use a write concern"
               (-> (unordered-bulk db :dummy)
                   (insert {:name "Joe"})
                   (execute! :fsynced)))
         (fact "Can use an ordered bulk as well"
               (-> (ordered-bulk db :dummy)
                   (insert {:name "Joe"})
                   (remove-doc {:name "Joe"})
                   (execute!))))

  (facts "Bulk delete operations"
         (against-background (before :facts (h/drop! db :dummy)))
         (fact "Can remove a document"
               (h/insert! db :dummy {:_id 1 :foo "a"})
               (-> (unordered-bulk db :dummy)
                   (remove-doc {:_id 1})
                   (execute!))
               (count (h/fetch-docs db :dummy {})) => 0)
         (fact "Remove all matching documents"
               (h/insert! db :dummy [{:_id 1 :foo "a"} {:_id 2 :foo "a"} {:_id 3 :foo "b"}])
               (-> (unordered-bulk db :dummy)
                   (remove-doc {:foo "a"})
                   (execute!))
               (h/fetch-docs db :dummy {}) => [{:_id 3 :foo "b"}])
         (fact "Remove-one remove only the first matching documents"
               (h/insert! db :dummy [{:_id 1 :foo "a"} {:_id 2 :foo "a"}])
               (-> (unordered-bulk db :dummy)
                   (remove-first-doc {:foo "a"})
                   (execute!))
               (h/fetch-docs db :dummy {}) => [{:_id 2 :foo "a"}]))

  (facts "Bulk update operations"
         (against-background (before :facts (do
                                              (h/drop! db :dummy)
                                              (h/insert! db :dummy {:_id 1 :foo "a" :bar "b"}))))
         (fact "Can update a field of an object"
               (-> (unordered-bulk db :dummy)
                   (update {:_id 1} {:foo "c"})
                   (execute!))
               (h/fetch-first-doc db :dummy {}) => (contains {:foo "c" :bar "b"}))
         (fact "Can update multiple fields of an object"
               (-> (unordered-bulk db :dummy)
                   (update {:_id 1} {:foo "a2" :bar "b2" :bazz "c2"})
                   (execute!))
               (h/fetch-first-doc db :dummy {}) => (contains {:foo "a2" :bar "b2" :bazz "c2"}))
         (fact "Can update the first found document"
               (h/insert! db :dummy {:_id 2 :foo "a" :bar "b"})
               (-> (unordered-bulk db :dummy)
                   (update-first {:foo "a"} {:bar "b2"})
                   (execute!))
               (h/fetch-docs db :dummy {} :order-by [:_id]) => [{:_id 1 :foo "a" :bar "b2"}, {:_id 2 :foo "a" :bar "b"}])
         (fact "Can replace a document"
               (-> (unordered-bulk db :dummy)
                   (replace-first {:_id 1} {:foo "a2"})
                   (execute!))
               (h/fetch-first-doc db :dummy {}) => {:_id 1 :foo "a2"})
         (fact "Can upsert an existing document"
               (-> (unordered-bulk db :dummy)
                   (update-or-insert {:_id 1} {:foo "a2"})
                   (execute!))
               (h/fetch-first-doc db :dummy {}) => {:_id 1 :foo "a2" :bar "b"})
         (fact "Can upsert a non existing document"
               (-> (unordered-bulk db :dummy)
                   (update-or-insert {:_id 2} {:foo "x"})
                   (execute!))
               (h/fetch-first-doc db :dummy {:_id 2}) => {:_id 2 :foo "x"})
         (fact "Update-one-or-insert updates only the first document"
               (h/insert! db :dummy {:_id 2 :foo "a" :bar "b"})
               (-> (unordered-bulk db :dummy)
                   (update-first-or-insert {:foo "a"} {:foo "a2"})
                   (execute!))
               (h/fetch-docs db :dummy {} :order-by [:_id]) => [{:_id 1 :foo "a2" :bar "b"} {:_id 2 :foo "a" :bar "b"}])
         (fact "Update-one-or-insert inserts if nothing is found"
               (-> (unordered-bulk db :dummy)
                   (update-first-or-insert {:_id "non existing"} {:foo "a2"})
                   (execute!))
               (h/fetch-first-doc db :dummy {:_id "non existing"}) => {:_id "non existing" :foo "a2"})
         (fact "Replace-one-or-insert replaces only the first document"
               (h/insert! db :dummy {:_id 2 :foo "a" :bar "b"})
               (-> (unordered-bulk db :dummy)
                   (replace-first-or-insert {:foo "a"} {:foo "a2"})
                   (execute!))
               (h/fetch-docs db :dummy {}) => [{:_id 1 :foo "a2"} {:_id 2 :foo "a" :bar "b"}])
         (fact "Replace-one-or-insert inserts if nothing is found"
               (-> (unordered-bulk db :dummy)
                   (replace-first-or-insert {:_id "non existing"} {:_id "non existing" :foo "a2"})
                   (execute!))
               (h/fetch-first-doc db :dummy {:_id "non existing"}) => {:_id "non existing" :foo "a2"})))
