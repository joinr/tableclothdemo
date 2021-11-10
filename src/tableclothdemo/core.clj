(ns tableclothdemo.core
  (:require [tablecloth.api :as tc]
            [tech.v3.dataset :as tmd]
            [tech.v3.datatype.functional :as dfn]))

(def raw-data
  "First	Last	Age	Location	Category
Bilbo	Baggins	400	The Shire	LOTR
S'chn T'gai	Spock	120	Vulcan	Trek
James	Kirk	49	Enterprise	Trek
Gordon	Shumway	285	Earth	ALF")

;;assuming we have some tab delimited data on disk [or at  url]...
(def file-path "data.txt")
(spit "data.txt" raw-data)

;;two other ways to "view" the data:
(def column-map {:First ["Bilbo" "S'chn T'gai" "James" "Gordon"],
                 :Last ["Baggins" "Spock" "Kirk" "Shumway"],
                 :Age [400 120 49 285],
                 :Location ["The Shire" "Vulcan" "Enterprise" "Earth"],
                 :Category ["LOTR" "Trek" "Trek" "ALF"]})

(def records [{:Last "Baggins", :Location "The Shire", :First "Bilbo", :Age 400, :Category "LOTR"}
              {:Last "Spock", :Location "Vulcan", :First "S'chn T'gai", :Age 120, :Category "Trek"}
              {:Last "Kirk", :Location "Enterprise", :First "James", :Age 49, :Category "Trek"}
              {:Last "Shumway", :Location "Earth", :First "Gordon", :Age 285, :Category "ALF"}])


;;we can create a tmd dataset using tc/dataset or tmd/->dataset which are loosely equivalent.
;;tablecloth tries to help us simplify some operations and provides a bit more API for
;;manipulating datasets akin to the dplyr library from R.

(def DS (tc/dataset file-path {:file-type :tsv :key-fn keyword}))
;;datasets can be ordered trivially:
(tc/order-by DS [:Category :Age])
;; #'tableclothdemo.core/DSdata.txt [4 5]:

;; |      :First |   :Last | :Age |  :Location | :Category |
;; |-------------|---------|-----:|------------|-----------|
;; |      Gordon | Shumway |  285 |      Earth |       ALF |
;; |       Bilbo | Baggins |  400 |  The Shire |      LOTR |
;; |       James |    Kirk |   49 | Enterprise |      Trek |
;; | S'chn T'gai |   Spock |  120 |     Vulcan |      Trek |


;;for now if we have the raw data as a csv string, we have to coerce it to an
;;input stream to work.  This is apparently less common than reading files
;;or urls etc.
(defn txt->input-stream [txt]
  (java.io.ByteArrayInputStream. (.getBytes raw-data)))


(def DS (tech.v3.dataset/->dataset (txt->input-stream raw-data) {:file-type :tsv :key-fn keyword}))
(tc/order-by DS :Age)
;; _unnamed [4 4]:

;; |      :First |   :Last | :Age |  :Location |
;; |-------------|---------|-----:|------------|
;; |       James |    Kirk |   49 | Enterprise |
;; | S'chn T'gai |   Spock |  120 |     Vulcan |
;; |      Gordon | Shumway |  285 |      Earth |
;; |       Bilbo | Baggins |  400 |  The Shire |


;;We can also use clojure idioms with a seq-of-maps approach, where
;;the dataset records (column names associated to row values for each row) are maps:

(= records (tmd/mapseq-reader DS))
;;true

(take 2 (tmd/mapseq-reader DS))
;; ({:Last "Baggins",
;;   :Location "The Shire",
;;   :First "Bilbo",
;;   :Age 400,
;;   :Category "LOTR"}
;;  {:Last "Spock",
;;   :Location "Vulcan",
;;   :First "S'chn T'gai",
;;   :Age 120,
;;   :Category "Trek"})

(->> DS
     tmd/mapseq-reader ;;not in tablecloth, in tech.v3.dataset.  equivalent to
     (sort-by :Age)    ;;standard clojure sort-by
     tmd/->dataset)    ;;coerce the sequence of maps back into a dataset

;; |  :Location |   :Last | :Category | :Age |      :First |
;; |------------|---------|-----------|-----:|-------------|
;; | Enterprise |    Kirk |      Trek |   49 |       James |
;; |     Vulcan |   Spock |      Trek |  120 | S'chn T'gai |
;; |      Earth | Shumway |       ALF |  285 |      Gordon |
;; |  The Shire | Baggins |      LOTR |  400 |       Bilbo |

;;in-memory representations also work:


(= (tc/dataset column-map) DS)
;;true
(= (tc/dataset records)       DS)
;;true
