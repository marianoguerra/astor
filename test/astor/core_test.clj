(ns astor.core-test
  (:refer-clojure :exclude (read))
  (:import [org.marianoguerra.astor Event])
  (:require [clojure.test :refer :all]
            [astor :refer :all]))

(defn make-new-chunk [name]
  (clojure.java.io/delete-file name true)
  (new-chunk name))

(defn normalize-event [event]
  (let [ev-bytes (to-event-clj event)]
    (assoc ev-bytes :data (java.util.Arrays/toString (:data ev-bytes)))))

(deftest smoke-tests
  (testing "a chunk can be created"
    (is (make-new-chunk "foo")))
  
  (testing "stats return all zero when chunk doesn't exist"
    (let [chunk (make-new-chunk "foo")
          stats (stats chunk)]
      (is (= stats {:size 0 :count 0 :oldest 0 :newest 0 :base-seqnum 0
                    :last-seqnum 0}))))

  (testing "writting 1 event and then getting stats reflects what's written"
    (let [chunk (make-new-chunk "foo")
          data (.getBytes "hello") 
          data-length (alength data)
          total-length (+ Event/HEADER_SIZE data-length
                          Event/HEADER_DATA_LENGTH_SIZE)
          event1 (write chunk 42 data)
          ev1 (to-event-clj event1)
          cstats (stats chunk)]

      (is (= cstats {:size total-length :count 1 :oldest 42 :newest 42 :base-seqnum 1
                     :last-seqnum 1}))
      (is (= ev1 {:timestamp 42 :size data-length :crc (:crc ev1) :seqnum 1
                  :data data}))
      ))

  (testing "writting 2 event and then getting stats reflects what's written"
    (let [chunk (make-new-chunk "foo")
          data-1 (.getBytes "hello") 
          data-2 (.getBytes "world") 
          data-length-1 (alength data-1)
          data-length-2 (alength data-2)
          total-length-1 (+ Event/HEADER_SIZE data-length-1
                            Event/HEADER_DATA_LENGTH_SIZE)
          total-length-2 (+ Event/HEADER_SIZE data-length-2
                            Event/HEADER_DATA_LENGTH_SIZE)
          event1 (write chunk 42 data-1)
          event2 (write chunk 43 data-2)
          ev1 (to-event-clj event1)
          ev2 (to-event-clj event2)
          cstats (stats chunk)]

      (is (= cstats {:size (+ total-length-1 total-length-2) :count 2
                     :oldest 42 :newest 43 :base-seqnum 1 :last-seqnum 2}))
      (is (= ev1 {:timestamp 42 :size data-length-1 :crc (:crc ev1) :seqnum 1
                  :data data-1}))
      (is (= ev2 {:timestamp 43 :size data-length-2 :crc (:crc ev2) :seqnum 2
                  :data data-2}))
      ))

  (testing "read-last on empty returns empty"
    (is (empty? (read-last (make-new-chunk "foo")))))

  (testing "read-last N on empty returns empty"
    (is (empty? (read-last (make-new-chunk "foo") 10))))

  (testing "read-last with 1 entry works"
    (let [chunk (make-new-chunk "foo")
          data (.getBytes "hello") 
          event1 (write chunk 42 data)
          ev1 (normalize-event event1)
          result (read-last chunk)
          result-event (normalize-event (first result))]

      (is (= (count result) 1))
      (is (= ev1 result-event))))

  (testing "read-last 5 with 1 entry works"
    (let [chunk (make-new-chunk "foo")
          data (.getBytes "hello") 
          event1 (write chunk 42 data)
          ev1 (normalize-event event1)
          result (read-last chunk 5)
          result-event (normalize-event (first result))]

      (is (= (count result) 1))
      (is (= ev1 result-event))))

  (defn with-2-read-last-n [n]
    (let [chunk (make-new-chunk "foo")
          data1 (.getBytes "hello") 
          event1 (write chunk 42 data1)
          ev1 (normalize-event event1)

          data2 (.getBytes "hi") 
          event2 (write chunk 43 data2)
          ev2 (normalize-event event2)

          result (read-last chunk n)
          result-event1 (normalize-event (first result))
          result-event2 (normalize-event (second result))]

      (is (= (count result) 2))
      (is (= ev1 result-event1))
      (is (= ev2 result-event2))))

  (testing "read-last 2 with 2 entries works"
    (with-2-read-last-n 2))

  (testing "read-last 5 with 2 entries works"
    (with-2-read-last-n 5))
  )
