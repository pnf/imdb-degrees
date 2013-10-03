(ns imdb.core
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
             [clojure.lang Keyword]))
(clojure.core.typed/typed-deps clojure.core.typed.async)

(t/ann ^:no-check monger.core/connect!
        (Fn [(HMap :optional {:port t/AnyInteger :host String} :complete? true) -> (t/Option com.mongodb.MongoClient)]
            [-> (t/Option com.mongodb.MongoClient)]))
(t/ann ^:no-check monger.core/get-db [String -> com.mongodb.DBApiLayer])
(t/ann ^:no-check monger.core/set-db! [com.mongodb.DBApiLayer -> com.mongodb.gridfs.GridFS])

(mg/connect! {:port 3333})
(mg/set-db! (mg/get-db "imdb"))

;; This signature allows unions of arbitrary types
(t/ann ^:no-check clojure.set/union (All [a] (Fn [(t/Set a) * -> (t/Set a)])))
;; This signature allows only same-type unions
#_(t/ann clojure.set/union (All [x [x1 :< x :> x]]
                          (Fn [(t/Set x) (t/Set x1) * -> (t/Set x1)])))


(t/ann ^:no-check org.httpkit.client/request ['{:url String :method (Value :get)}, [Any -> Any] -> (t/Atom1 '{:status Number :body String})])
(t/ann  get-or-throw [String -> '{:status Number :body String}])
(defn get-or-throw [url]
  (let [doc (http/request {:url url :method :get} identity)
        doc @doc
        _ (if (not= (:status doc) 200) (throw (Exception. (str "Got bad status: " doc))))]
    doc
    ))

(t/def-alias Node "Node representing an actor or film"
  (HMap :mandatory {:id String 
                    :links (t/Set String)
                    :title String
                    :type (U ':actor ':film)}
        :optional {:distance t/AnyInteger
                   :path (t/Seq String)
                   :release-date String
                   :genre String
                   :runtime String
                   :metacritic-score Integer}
        :complete? true
))

(t/def-alias Graph "Graph of tokens to nodes" (t/Map String Node))


;; Type-correct assignments to node fields within a graph
(t/ann ^:no-check set-node
       (Fn [Graph String (U ':title ':id) String -> Graph]
           [Graph String ':distance t/AnyInteger -> Graph]
           [Graph String ':links (t/Set String) -> Graph]
           [Graph String ':visited Boolean -> Graph]
           [Graph String ':path (t/Coll String) -> Graph]))
(defn set-node [g id field v] (assoc-in g [id field] v))

(t/ann ^:no-check assoc-node
       (Fn [Node ':distance t/AnyInteger -> Node]
           [Node ':path     ( t/Coll String) -> Node]
           [Node ':metacritic-score t/AnyInteger -> Node]
           [Node (U ':runtime ':genre ':release-date) String -> Node]
           [Node ':updated Boolean -> Node]))
(def assoc-node assoc)

;; The only ambiguity for core.typed is whether or not this will return nil.
(t/non-nil-return java.lang.String/startsWith :all)

(t/def-alias Resource "Parsed DOM resource"
  (Rec [x] (t/Map Keyword (U String x (t/Seq x)))))
(t/ann ^:no-check net.cgrand.enlive-html/html-resource [java.io.Reader -> Resource])
(t/ann ^:no-check net.cgrand.enlive-html/select [Resource (t/Vec Keyword) -> (t/Seq Resource) ])

(t/ann get-resource-or-throw [String -> Resource])
(defn get-resource-or-throw [url]
  (-> url
      get-or-throw
      (as-> % (html/html-resource (java.io.StringReader. (:body %))))))

;(t/ann clojure.core/set [ (t/Seqable Any) -> t/Set])
(t/ann ^:no-check clojure.string/replace 
       [String
        (U String Character java.util.regex.Pattern) 
        (U String Character [(t/Seq String) -> String]) -> String ])

(t/ann resource->title [Resource -> String])
(defn resource->title [doc]
  (-> doc
      (as-> % (:content (first (html/select % [:title]))) )
      (as-> % (clojure.string/replace (apply str %) #"\n" ""))))

;; Extract a list of the hrefs over a bunch of items like this:
;;     <div class="title"> 
;;        <a href="/title/tt0060438/" onClick="_gaq.push(['_trackEvent', 'Find', '', '']);">A Funny Thing Happened on the Way to the Forum</a> (1966)
;;    </div>
;; Punt on type-checking for the deep structure of the DOM.
(t/ann ^:no-check resource->links [String Resource -> (t/Set String)])
(defn resource->links [id resource]
  (-> resource
      (html/select [:div.title])
      ;; Turn into list of pairs [href, title]
      (as-> doc (map (t/fn> :- (t/Seq String) [e :- Resource]
                            [(-> e :content second :attrs :href)
                             (-> e :content last)]) doc))
      ;; If this is a list of performances, keep only ones that look like movies
      (as-> doc (if (.startsWith ^String id "/name")
                  (filter #(re-find #"\(\d\d\d\d\)" (last %)) doc)
                  doc))
      ;; turn into list of hrefs
      (as-> doc (map first doc))
      set))

(t/ann ^:no-check merged-links [(t/Seq Node) -> (t/Set String)])
(defn merged-links [nodes]  
  (apply clojure.set/union (map :links nodes)))

(t/ann ^:no-check extract-details [Resource -> (t/Map String String)])
(defn extract-details [res]
  (-> res
      (html/select [:section.details :div #{:h1 :p}])
      (as-> % (map (comp first :content) %))
      (as-> % (apply hash-map %))))

(t/ann field-map (t/Map String Keyword))
(def field-map {"Run time" :runtime
                "Genre"    :genre
                "Release Date" :release-date})

(t/ann add-film-details [Node -> Node])
(defn  add-film-details [node]
  (if (every? #(get node %) [:runtime :genre :release-date]) node
      (let [url     (str "http://m.imdb.com" (:id node))
            res     (get-resource-or-throw url)
            details (extract-details res)]
        (-> node
            (assoc-node :runtime (or (details "Run time") "na"))
            (assoc-node :genre   (or (details "Genre") "na"))
            (assoc-node :release-date (or (details "Release Date") "na"))
            (assoc-node :updated true)))))

(t/ann ^:no-check extract-metacritic-score [Resource -> Integer])
(defn extract-metacritic-score [res]
  (try (-> res
           (html/select [:div.metascore_wrap :div :span])
           first :content first Integer/parseInt)
       (catch NumberFormatException e -1)))

(t/ann add-metacritic-score [Node -> Node])
(defn add-metacritic-score [node]
  (if (get node :metacritic-score) node
   (let [url (str "http://www.imdb.com" (:id node) "criticreviews")
         doc (get-or-throw url)
         res (html/html-resource (java.io.StringReader. (:body doc)))
         ms  (extract-metacritic-score res)]
     (-> node
         (assoc-node :metacritic-score ms)
         (assoc-node :updated true)))))

(t/ann fetch-links-from-imdb [String -> Node])
(defn fetch-links-from-imdb
  "Attacks the mobile version of the IMDB site to pull down information about an
  actor or film, to be specified like id=/name/nm123455/ or /title/tt2343243/
  (note slashes).  Returns vector of [title [links]] where links are in the same
  format as the id.  Links are to films if the id was an actor, or to actors if the
  id was a film.  In the former case, we attempt to reap only films and not TV shows."
  [id]
  (let [tp   (if  (.startsWith ^String id "/name") :actor :film) 
        urls (if  (= tp :actor)
                [(str "http://m.imdb.com" id "filmotype/actor")
                 (str "http://m.imdb.com" id "filmotype/actress")]
                [ (str "http://m.imdb.com" id "fullcredits/cast")])
         _ (println "Fetching" urls)
         docs (t/for> :- Node
                      [url :- String urls]
                      (let [doc (get-or-throw url)
                            doc (html/html-resource (java.io.StringReader. (:body doc)))
                            title  (resource->title doc)
                            links (resource->links id doc)
                            _ (println "Fetched" title)]
                        {:title title :links links :id id :type tp}))
         title   (or (:title (first docs)) "Not found" )
         links   (merged-links docs)]
     {:title title :links links :id id :type tp}))

(t/ann fetch-node-from-imdb [String -> Node])
(defn  fetch-node-from-imdb [id]
  (let [node (fetch-links-from-imdb id)
        tp   (:type node)]
    (if (= tp :film)
      (-> node
          (add-metacritic-score)
          (add-film-details))
      node)))

; Restrict links for testing.
;(def allowed-links #{"/name/nm0000257/" "/name/nm0748620/" "/title/tt1655460/"})
;(defn restrict-links [links]  (filter #(allowed-links %) links))

(t/def-alias MongoRecord "Mongo key value pairs"
  (clojure.lang.IPersistentMap Any Any))

(t/ann ^:no-check assure-node [MongoRecord -> Node])
(defn assure-node [r]
  (assert (string? (:id r)))
  (assert (string? (:title r)))
  (assert (coll? (:links r)))
  ;(assert (every? string? (:links r)))
  r)


(t/ann ^:no-check monger.collection/find-maps 
       (Fn  [String  MongoRecord -> (t/Seq MongoRecord)]
            [String -> (t/Seq MongoRecord)]))
(t/ann ^:no-check monger.collection/insert-and-return  [String MongoRecord -> MongoRecord])
(t/ann fetch-node-from-mongo [String -> Node])
(defn fetch-node-from-mongo [id]
  (let [node (assure-node (or (first (mc/find-maps "nodes" {:id id}))
                              (mc/insert-and-return "nodes" (fetch-node-from-imdb id))
                              ))]
    (assure-node (-> node 
                     (assoc-node :distance 999999)
                     (assoc-node :path (list id))))))

(t/ann fetch-node-from-graph [Graph String -> (Vector* Graph Node)])
(defn fetch-node-from-graph
  "Fetch a node from the current graph, adding it from imdb if necessary.
   Entries in the graph are id ==> {:id id :links links :title title :distance dist },
   where distance is initialized to be something large.
   Returns [newgraph entry]."
  [graph id]
  (if-let [node (graph id)]
    [graph node]
    (let [node (fetch-node-from-mongo id)]
      [(assoc graph id node) node])))

(t/ann distance [Node -> t/AnyInteger])
(defn distance [node] (or (:distance node) 999999999))

(t/ann update-distance [Node Graph String -> Graph])
(defn update-distance
  "Update :distance field in graph node id to be one greater than di (if that will reduce it).
Returns the updated graph."
  [entry graph id]
  (let [di (distance entry)
        [graph node] (fetch-node-from-graph graph id)
        path (or (:path entry) '())]
    (if (<= (distance node) di)  graph
                (-> graph
                    (set-node id :distance (inc di))
                    (set-node id :path (conj path id))))))

(t/ann-datatype clojure.data.priority-map.PersistentPriorityMap
                [priority->set-of-items :- (HMap)
                 item->priority :- (HMap)
                 _meta :- (HMap)])


(t/def-alias Queue (t/Map String t/AnyInteger))
(t/ann ^:no-check clojure.data.priority-map/priority-map [ -> Queue])
(t/ann ^:no-check pmap-assoc [Queue String (U nil t/AnyInteger) -> Queue])
(def pmap-assoc assoc)
(t/ann ^:no-check pmap-peek [Queue -> String])
(def pmap-peek (comp first peek))
(t/ann ^:no-check pmap-pop [Queue -> Queue])
(def pmap-pop pop)

(t/ann update-queue [Queue Graph String (t/Seqable String) -> Queue])
(defn update-queue [queue graph id links] 
  (reduce (t/fn> :- Queue [q :- Queue id :- String]
                 (pmap-assoc q id (:distance (get graph id)))) queue links))

; visit node and update distances in all unvisited, connected nodes
(t/ann visit-node [Graph Queue String -> (Vector* Graph Queue)])
(defn visit-node
  "Visits node id, and update the :distance fields of its neighbors.
   queue is a priority queue of id=>distance, containing entries only for unvisited
   nodes.  We will use this to determine what to visit next, Dijkstra style."
  [graph queue id]
  (let [[graph entry] (fetch-node-from-graph graph id)
        links	      (filter #(nil? (:visited (graph %))) (:links entry))
        graph         (reduce (partial update-distance entry) graph links)
        queue         (update-queue queue graph id links)
        graph         (set-node graph id :visited true)]
    [graph queue]))


(t/ann find-distance [String String -> (t/Option Any)])
(defn find-distance 
  "E.g. (find-distance \"/name/nm0000257/\" \"/name/nm0000295/\")
   Use Dijkstra algorithm to find shortest path from id to target."
  [id target]
    (let [ [graph node]  (fetch-node-from-graph {} id)
           graph         (set-node graph id :distance 0)
           queue         (priority-map)]
      (t/loop> [graph :- Graph graph
                queue :- Queue queue
                id    :- String id]
        (if (= id target) (get graph id)
          (let [[graph queue]   (visit-node graph queue id)
                closest         (pmap-peek queue)]
            (if (empty? queue)
                "Couldn't find a path!"
                (recur graph (pmap-pop queue) closest)))))))

; /name/nm0000148/ harrison ford

(t/ann fetch-with-backoff [String -> Node])
(defn fetch-with-backoff [id]
  (loop [w 1]
    (or (try (fetch-node-from-mongo id)
             (catch Exception _ nil))
        (do 
          (println "Sleeping" w "after exception from" id)
          (Thread/sleep (* 1000 w))
          (recur (* 2 w))))))

; Follow the tree only if there's a valid metacritic score, or this
; is an actor with more than 10 films.
(defn with-metacritic [node]
  (or 
   (and (:metacritic-score node)
        (>= (:metacritic-score node) 0))
   (and (seq (:links node))
        (> (count (:links node)) 5))))

(defn pillage [id pred]
  (loop [[id & ids] (list id nil)
         already    #{}]
    (let [node  (fetch-with-backoff id)
          links (filter #(not (already %))  (:links node))
          ids   (if (and (pred node) (seq links))
                  (apply conj ids links) ids)]
      (when (seq ids) (recur ids (conj already id))))))
