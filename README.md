# Humongous a Mongo DB client

Humongous, humongous is your wishful servant to help you speaking to dear Mongo DB.

Simplicity in mind, but right beneath the surface, access to all the powers of the Java driver.

Expect a document API to make a document come to existence, change its appearance or decease. 

But this is not all humongous. A servant is ready, to be sent to the database to change what can be found,
  to remove what should be hidden, or to add what is missing. 
  
And humongous, if this is not yet enough, a bulk of servants is there to serve in parallel
  or to transport many of your wishes and fulfill them one by one.
 
## A note of caution

The API is not yet stable. You might consider to wait for version 0.1.

TODO

- Allow the command API without batch
- Map, reduce pipelines
- DB operations 

[![Clojars Project](http://clojars.org/humongous-client/latest-version.svg)](http://clojars.org/humongous-client)

 
## Quick tour of document API in the REPL

Open a connection to a database

    (require '[humongous.db :as mongodb])

    (def db (mongodb/create-db-client "mongodb://localhost:27017/mydatabase"))
    
    ; it is thread safe, you need only one and you should close it when the application shuts down
    (mongodb/close! db)
    
**Insert** one or many documents
    
    (insert! db :kites {:name "Blue"})

    (insert! db :kites [{:_id 1 :name "blue"} {:_id 2 :name "red"}])

**Update** fields of a document or the document as a whole
                         
    ;---> Update field name to green of all matching documents
    (update! db :kites {:_id 123 :name "blue"})
      
    ;---> Update field name to green of matching document
    (update-fields! db :kites {:_id 123 :name "blue"} {:name "green"})


**Remove** document with id 123
      
      (remove! db :kites {:_id 123 :name "blue"})
                                    
**Fetch** documents

    ;---> fetch kites with name *red*
    (fetch-docs db :kites {:name "red"})
    
    ;---> Fetch just the first matching
    (fetch-first-doc db :kites {:name "green"})
    
    ;---> Use the Mongo operator  http://docs.mongodb.org/manual/tutorial/query-documents/
    (fetch-docs db :kites {:size {:$lt 7}})
    
    ;---> Select the request fields and sort the result
    (fetch-docs db :kites {:name "blue"} :fields [:name] :order-by [[:name :desc]])

## Optimistic locking for the document API

Update and remove operation check only the _id by default. If you wrap the operation with *(optimistic ..)*,
 the fields *_id and _version* are checked. If the *_version* is different, or the document is missing,
  the operation return false, else it bumps up the version in one go and returns the *WriteResult*.
 
    (optimistic (update! db :kites {:_id 1 :_version 1}))
    ; -> false 
    ; no kite no update
     
    (insert! db :kites {:_id 1 :name "blue" :_version 2})
    (optimistic (update! db :kites {:_id 1 :_version 1}))
    ; -> false 
    ; wrong version
     
    (optimistic (update! db :kites {:_id 1 :name "red" :_version 1}))
    ; -> #<WriteResult { "serverUsed" : "localhost:27017" , "ok" : 1 , "n" : 1}>
    ; success, version is bumped up as well
    (fetch-first-doc db :kites {})
    ; -> {:_id 1 :name "red" :_version 2}
                                                        
## Write concerns
 
The Mongo DB support various write concerns. For example to guaranty that the document is written to the primary and one
replicated DB, or to the majority, or ...

You can find the supported write concerns in: *humongous.db#write-concerns*

    (insert! db :kites {:_id 1 :name "blue" :size 5})
    (update! db :kites {:_id 1 :name "red"} :unacknowledged)
      
Write concerns are supported for
      
- insert!
- update!
- update-fields!
- remove!

If no concern is specified, the write concern is taken from the collection, the database or the connection.

Mongos default is :unacknowledged

You can specify a default concern while connecting
    
    (def db (create-db-client "mongodb://localhost:27017/test" :write-concern :acknowledged))    
    
## Command API

Mongo supports ordered and unordered bulk operations. Unordered can be executed in parallel, ordered only sequentially.

    (require '[humongous.humongous :refer :all])
    (-> (unordered-bulk db :dummy)
      (insert {:_id 1 :foo "a"})
      (update {:_id 3} {:foo "a2" :bar "b2" :bazz "c2"})
      (update-first {:foo "a"} {:bar "b2"})
      (update-first-or-insert {:_id "non existing"} {:foo "a2"})
      (update-or-insert {:_id 2} {:foo "x"})
      (remove-first-doc {:foo "a"})
      (remove-doc {:foo "b"})
      (execute!))
    (-> (ordered-bulk db :dummy)
      (insert {:name "Joe"})
      (remove-doc {:name "Joe"})
      (execute!)))

## Internal API
 
*(fetch-docs ...)* uses chained function calls internally, which operate mostly on the *DBCursor*. 

You can use them as well

    (ensure-index db :kites {:name 1})
     (->
       (query db :kites {:size 7})
        (order-by [:name])
        (skip 1)
        (limit 1)
        (query-comment "This query might be slow")
        (batch-size 10)
        (timeout-millis 2000)
        (query-hint {:name 1})
        (fetch))) 

Explain the server's query plan

    (->
      (query db :kites {:size 7})
      (explain)))
       
Count rows on the server       

     (->
       (query db :kites {:size 7})
       (count-rows)))
       
Fall back to Java method calls, if something is missing

    (defn add-comment [cursor c]
      (.comment cursor c))
    
    (ensure-index db :kites {:name 1})
     (->
       (query db :kites {:size 7})
       (add-comment "look at this query it is slow")
       (fetch))) 
       
       

## More documentation
                                                                                             
Please look into the tests.
                                                                                             
## How to run the tests

`lein midje` will run all tests.

`lein midje namespace.*` will run only tests beginning with "namespace.".

`lein midje :autotest` will run all the tests indefinitely. It sets up a
watcher on the code files. If they change, only the relevant tests will be
run again.
