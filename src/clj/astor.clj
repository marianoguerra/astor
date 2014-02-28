(ns astor
  (:refer-clojure :exclude (read))
  (:import [org.marianoguerra.astor Event Chunk EventHolder EventStore]))

;(defn ^Bucket root-bucket [path]
;  (Bucket. path))

;(defn ^Bucket new-bucket [^Bucket bucket name]
;  (.getBucket bucket name))

;(defn ^Stream new-stream [^Bucket bucket name]
;  (.getStream bucket name))

(defn to-event-clj [^Event event]
  {:timestamp (.timestamp event)
   :data (.data event)
   :seqnum (.seqnum event)
   :size (.size event)
   :crc (.crc event)})

(defn ^Chunk new-chunk [^String path]
  (Chunk. (.toPath (java.io.File. path))))

(defn read [^EventStore store &[seqnum limit]]
  (if (nil? seqnum)
    (.readLast store)
    (if (nil? limit)
      (.read store seqnum)
      (.read store seqnum limit))))

(defn read-last [^EventStore store &[limit]]
  (if (nil? limit)
    (.readLast store 1)
    (.readLast store limit)))

(defn write [^EventStore store timestamp data]
  (.write store timestamp data))

(defn stats [^EventHolder event-holder]
  {:size (.size event-holder)
   :count (.count event-holder)
   :oldest (.oldest event-holder)
   :newest (.newest event-holder)
   :base-seqnum (.baseSeqNum event-holder)
   :last-seqnum (.lastSeqNum event-holder)})
