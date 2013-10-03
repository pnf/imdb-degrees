(ns imdb.perceptron
  (:require [clojure.data.priority-map :refer [priority-map]]
             [org.httpkit.client :as http]
             [net.cgrand.enlive-html :as html]
             [net.cgrand.xml :as xml]
             [monger.core :as mg]
             [monger.collection :as mc]
             [monger.operators :as mo]
             [clojure.core.typed :as t]
             [clojure.core.async :as a]
             [clojure.core.typed.async :as ta])
  (:import [com.mongodb MongoOptions ServerAddress]
             [clojure.lang Keyword])
  (:use imdb.core))
(clojure.core.typed/typed-deps clojure.core.typed.async)

(t/ann ^:no-check monger.operators/$exists clojure.lang.Symbol)
(t/ann ^:no-check monger.operators/$regex clojure.lang.Symbol)
;; (t/ann ^:no-check clojure.core.async/chan
;;        (Fn [-> clojure.core.async.impl.channels.ManyToManyChannel]
;;            [(U clojure.core.async.impl.buffers.FixedBuffer t/Num)
;;             -> clojure.core.async.impl.channels.ManyToManyChannel]))
;; Enrich old mongo db with metacritic score, etc.
(t/ann repair-mongo (Fn [t/AnyInteger -> nil]))
(defn repair-mongo [n]
  (let [recs (mc/find-maps "nodes"
                           {:metacritic-score {mo/$exists false}
                            :id {mo/$regex "^\\/title"}})
        _     (println "Found" (count recs) "nodes to repair")
        c     (ta/chan> MongoRecord)]
    (t/dotimes> [i n] 
      (ta/go>
       (loop [] (when-let [rec (a/<! c)] 
                  (-> (dissoc rec :updated)
                      (assure-node)
                      (add-film-details)
                      (add-metacritic-score)
                      (as-> % (if (get % :updated)
                                (do (println "Updating in" i ":" %)
                                    (mc/update "nodes" {:id (:id %)} 
                                               (dissoc % :updated))))))
                  (recur)))
       (println "Leaving" i)))
    ;(a/onto-chan c recs) ;; want this to block
    (t/doseq> [rec :- MongoRecord, recs] (a/>!! c rec))))


;; now for the perceptron
;; Work on all films for which there are metacritic scores
;; Features are actors
;; Category

(t/def-alias Category t/AnyInteger)
(t/def-alias A2C2W (t/Map String (t/Vec  t/Num)))
(t/def-alias C2W (t/Vec t/Num))
(t/ann ^:no-check clojure.core/max-key (All [A] [[A -> t/Num] -> A]))

(t/ann ^:no-check imax [C2W -> Long])
(defn imax [c] (second (apply max-key first (map vector c (range (count c))))))

(t/ann ncats Long)
(def ncats 25)

(t/ann node->cat [Node -> Long])
(defn node->cat [node]  (long (/ (or (get node :metacritic-score) 0) ncats)))

(t/ann guess-cat [A2C2W Node -> Long])
(defn guess-cat [acw node]
  (imax (reduce (t/fn> :-  C2W
                       [wa :- C2W ws :- (t/Option C2W)]
                       (if ws (vec (map + wa ws)) wa))
                (vec (repeat ncats 0))
                (map (t/fn> :- (t/Option C2W) [a :- String] (get acw a))
                     (:links node)))))

(t/ann ^:no-check update-in-a
       [A2C2W (Vector* String) [(t/Option C2W) -> C2W] 
        -> A2C2W])
(def update-in-a update-in)
(t/ann ^:no-check update-in-w
       [C2W (Vector* Long) [t/Num -> t/Num] -> C2W])
(def update-in-w update-in)

(t/ann update-weights [A2C2W Node Long Long -> A2C2W])
(defn update-weights [acw node iright iwrong]
  (reduce (t/fn> :- A2C2W  [acw :- A2C2W, a :- String]
                 (update-in-a acw [a] 
                              (t/fn> :- C2W [ws :- (t/Option C2W)] 
                                     (-> (or ws (vec (repeat ncats 0)))
                                         (update-in-w [iright] inc)
                                         (update-in-w [iwrong] dec)))))
          acw (:links node)))

(t/ann train [t/AnyInteger (t/Seqable Node) -> A2C2W])
(defn train [n nodes]
  (reduce (t/fn> :- A2C2W [acw :- A2C2W,  node :- Node]
            (let [cat    (node->cat node)
                  guess  (guess-cat acw node)]
              (if (= cat guess) acw
                  (update-weights acw node cat guess))))
          {} (apply concat (repeatedly n #(shuffle nodes)))))

(t/ann ^:no-check rms [A2CW (t/Seqable Node) -> t/Num])
(defn rms [acw nodes]
  (let [es   (map (fn [node] (double (- (guess-cat acw node) (node->cat node)))) nodes)
        n    (count nodes)
        e1   (/ (reduce + es) n)
        es   (map #(- % e1) es)]
    (Math/sqrt (/ (reduce + (map * es es)) n))))


#_(def nodes (mc/find-maps "nodes"
                           {:metacritic-score {mo/$gte 0}
                            :id {mo/$regex "^\\/title"}}))
