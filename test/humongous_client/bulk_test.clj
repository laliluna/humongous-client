(ns humongous-client.bulk-test
  (:require [humongous-client.bulk :refer [unordered-bulk ordered-bulk insert
                                           update update-first replace-first
                                           update-or-insert update-first-or-insert replace-first-or-insert
                                           remove-doc remove-one-doc
                                           execute!]]
            [midje.sweet :refer :all]
            [humongous-client.humongous :as m]
            [humongous-client.db :as mongodb]))

(mongodb/with-open!
  [db (mongodb/create-db-client "mongodb://localhost:27017/test")]

  (facts "Bulk insert operations" (against-background
                                    (before :facts
                                            (m/with-db db (m/drop! :dummy))))
         (fact "Can execute one insert"
               (m/with-db db
                          (-> (unordered-bulk :dummy)
                              (insert {:_id 1 :foo 2})
                              (execute!)))
               (first (m/with-db db (m/fetch-docs :dummy {}))) => {:_id 1 :foo 2})
         (fact "Can execute multiple insert"
               (m/with-db db (-> (unordered-bulk :dummy)
                                 (insert {:_id 1 :foo "a"})
                                 (insert {:_id 2 :foo "b"})
                                 (execute!)))
               (m/with-db db (m/fetch-docs :dummy {})) =>
               '({:_id 1 :foo "a"} {:_id 2 :foo "b"}))
         (fact "Can insert collection"
               (m/with-db db (-> (unordered-bulk :dummy)
                                 (insert [{:_id 1 :foo "a"}, {:_id 2 :foo "b"}])
                                 (execute!)))
               (m/with-db db (m/fetch-docs :dummy {})) =>
               '({:_id 1 :foo "a"} {:_id 2 :foo "b"}))
         (fact "Can use a write concern"
               (m/with-db db
                          (-> (unordered-bulk :dummy)
                              (insert {:name "Joe"})
                              ; (execute! :fsynced) FIXME
                              (execute!))))
         (fact "Can use an ordered bulk as well"
               (m/with-db db
                          (-> (ordered-bulk :dummy)
                              (insert {:name "Joe"})
                              (remove-doc {:name "Joe"})
                              (execute!)))))

  (facts "Bulk delete operations"
         (against-background (before :facts (m/with-db db (m/drop! :dummy))))
         (fact "Can remove a document"
               (m/with-db db
                          (m/insert! :dummy {:_id 1 :foo "a"})
                          (-> (unordered-bulk :dummy)
                              (remove-doc {:_id 1})
                              (execute!))
                          (count (m/fetch-docs :dummy {})) => 0))
         (fact "Remove all matching documents"
               (m/with-db db
                          (m/insert! :dummy [{:_id 1 :foo "a"} {:_id 2 :foo "a"} {:_id 3 :foo "b"}])
                          (-> (unordered-bulk :dummy)
                              (remove-doc {:foo "a"})
                              (execute!))
                          (m/fetch-docs :dummy {}) => [{:_id 3 :foo "b"}]))
         (fact "Remove-one remove only the first matching documents"
               (m/with-db db
                          (m/insert! :dummy [{:_id 1 :foo "a"} {:_id 2 :foo "a"}])
                          (-> (unordered-bulk :dummy)
                              (remove-one-doc {:foo "a"})
                              (execute!))
                          (m/fetch-docs :dummy {}) => [{:_id 2 :foo "a"}])))

  (facts "Bulk update operations"
         (against-background (before :facts (do
                                              (m/with-db db (m/drop! :dummy)
                                                         (m/insert! :dummy {:_id 1 :foo "a" :bar "b"})))))
         (fact "Can update a field of an object"
               (m/with-db db
                          (-> (unordered-bulk :dummy)
                              (update {:_id 1} {:foo "c"})
                              (execute!))
                          (m/fetch-first-doc :dummy {}) => (contains {:foo "c" :bar "b"})))
         (fact "Can update multiple fields of an object"
               (m/with-db db
                          (-> (unordered-bulk :dummy)
                              (update {:_id 1} {:foo "a2" :bar "b2" :bazz "c2"})
                              (execute!))
                          (m/fetch-first-doc :dummy {}) => (contains {:foo "a2" :bar "b2" :bazz "c2"})))
         (fact "Can update the first found document"
               (m/with-db db
                          (m/insert! :dummy {:_id 2 :foo "a" :bar "b"})
                          (-> (unordered-bulk :dummy)
                              (update-first {:foo "a"} {:bar "b2"})
                              (execute!))
                          (m/fetch-docs :dummy {}) => [{:_id 1 :foo "a" :bar "b2"}, {:_id 2 :foo "a" :bar "b"}]))
         (fact "Can replace a document"
               (m/with-db db
                          (-> (unordered-bulk :dummy)
                              (replace-first {:_id 1} {:foo "a2"})
                              (execute!))
                          (m/fetch-first-doc :dummy {}) => {:_id 1 :foo "a2"}))
         (fact "Can upsert an existing document"
               (m/with-db db
                          (-> (unordered-bulk :dummy)
                              (update-or-insert {:_id 1} {:foo "a2"})
                              (execute!))
                          (m/fetch-first-doc :dummy {}) => {:_id 1 :foo "a2" :bar "b"}))
         (fact "Can upsert a non existing document"
               (m/with-db db
                          (-> (unordered-bulk :dummy)
                              (update-or-insert {:_id 2} {:foo "x"})
                              (execute!))
                          (m/fetch-first-doc :dummy  {:_id 2})) => {:_id 2 :foo "x"})
         (fact "Update-one-or-insert updates only the first document"
               (m/with-db db
                          (m/insert! :dummy {:_id 2 :foo "a" :bar "b"})
                          (-> (unordered-bulk :dummy)
                              (update-first-or-insert {:foo "a"} {:foo "a2"})
                              (execute!))
                          (m/fetch-docs :dummy {}) => [{:_id 1 :foo "a2" :bar "b"} {:_id 2 :foo "a" :bar "b"}]))
         (fact "Update-one-or-insert inserts if nothing is found"
               (m/with-db db
                          (-> (unordered-bulk :dummy)
                              (update-first-or-insert {:_id "non existing"} {:foo "a2"})
                              (execute!))
                          (m/fetch-first-doc :dummy  {:_id "non existing"})) => {:_id "non existing" :foo "a2"})
         (fact "Replace-one-or-insert replaces only the first document"
               (m/with-db db
                          (m/insert! :dummy {:_id 2 :foo "a" :bar "b"})
                          (-> (unordered-bulk :dummy)
                              (replace-first-or-insert {:foo "a"} {:foo "a2"})
                              (execute!))
                          (m/fetch-docs :dummy {}) => [{:_id 1 :foo "a2"} {:_id 2 :foo "a" :bar "b"}]))
         (fact "Replace-one-or-insert inserts if nothing is found"
               (m/with-db db
                          (-> (unordered-bulk :dummy)
                              (replace-first-or-insert {:_id "non existing"} {:foo "a2"})
                              (execute!))
                          (m/fetch-first-doc :dummy  {:_id "non existing"})) => {:_id "non existing" :foo "a2"})))
