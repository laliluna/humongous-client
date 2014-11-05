# humongous-client

Humongous, humongous is your wishful servant to help you speaking to dear Mongo DB.

Simplicity in mind, but right beneath the surface, access to all the powers of the Java driver.

Expect a document API to make a document come to existence, change its appearance or decease. 

But this is not all humongous. A servant is ready, to be sent to the database to change what can be found,
  to remove what should be hidden, or to add what is missing. 
  
And humongous, if this is not yet enough, a bulk of servants is there to serve in parallel
  or to transport many of your wishes and fulfill them one by one.
 
## Quick tour of document API in the REPL

Open a connection to a database

    (require '[humongous.db :as mongodb])

    (def db (mongodb/create-db-client "mongodb://localhost:27017/mydatabase"))
    
    ; it is thread safe, you need only one and you should close it when the application shuts down
    (mongodb/close! db)

Wrap everything in *with-db*

    (require '[humongous.humongous :refer :all])


    (with-db db 
      (insert! ...) 
      (update! ...) 
      (fetch-docs ...))
    
**Insert** one or many documents
    
    (with-db db (insert! :kites {:name "Blue"}))

    (with-db db 
      (insert! :kites [{:_id 1 :name "blue"} {:_id 2 :name "red"}]))

**Update** fields of a document or the document as a whole
                         
    (with-db db 
      ;---> Update field name to green of all matching documents
      (update! :kites {:_id 123 :name "blue"} )
      
      ;---> Update field name to green of first matching document
      (update-first! :kites {:_id 123 :name "blue"} {:name "green"})


**Remove** document with id 123
      
      (with-db db
        (remove! :kites {:_id 123 :name "blue"))
                                    
**Fetch** documents

    ;---> fetch kites with name *red*
    (with-db db (fetch-docs :kites {:name "red"})
    
    ;---> Fetch just the first matching
    (with-db db (fetch-first-doc :kites {:name "green"}))
    
    ;---> Use the Mongo operator  http://docs.mongodb.org/manual/tutorial/query-documents/
    (with-db db (fetch-docs :kites {:size {:$lt 7}})
    
    ;---> Select the request fields and sort the result
    (with-db db (fetch-docs :kites {:name "blue"} :fields [:name] :order-by [[:name :desc]]))

## Internal API
 
*(fetch-docs ...)* uses chained function calls internally, which operate mostly on the *DBCursor*. 

You can use them as well

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
        (fetch))) 

Explain the server's query plan

    (with-db db
     (->
       (query :kites {:size 7})
       (explain)))
       
Count rows on the server       

    (with-db db
     (->
       (query :kites {:size 7})
       (count-rows)))
       
Fall back to Java method calls, if something is missing

    (defn add-comment [cursor c]
      (.comment cursor c))
    
    (with-db db
     (ensure-index :kites {:name 1})
     (->
       (query :kites {:size 7})
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
