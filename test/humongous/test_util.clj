(ns humongous.test-util)

(defn get-db-uri
  ([] (or (System/getenv "mongo_test_db")  "mongodb://localhost:27017/"))
  ([db] (str (get-db-uri) db)))

