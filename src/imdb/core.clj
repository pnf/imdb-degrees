(ns imdb.core
  (:require [clojure.data.priority-map :refer [priority-map]]
             [org.httpkit.client :as http]
             [net.cgrand.enlive-html :as html]
             [monger.core :as mg]
             [monger.collection :as mc]
             [clojure.core.typed :as t]
)
  (:import [com.mongodb MongoOptions ServerAddress]))

(t/ann ^:no-check monger.core/connect! [Any -> Any])
(t/ann ^:no-check monger.core/set-db! [Any -> Any])
(t/ann ^:no-check monger.core/get-db [Any -> Any])
(mg/connect! {:port 3333})
(mg/set-db! (mg/get-db "imdb"))
(t/ann ^:no-check clojure.set/union (Fn [t/Set * -> t/Set]))

(t/ann ^:no-check clojure.core/assoc-in [(HMap) (t/Vec Any) Any -> (HMap)])


(t/ann ^:no-check org.httpkit.client/request ['{:url String :method (Value :get)}, [Any -> Any] -> (t/Atom1 '{:status Number :body String})])
(t/ann  get-or-throw [String -> '{:status Number :body String}])
(defn get-or-throw [url]
  (let [doc (http/request {:url url :method :get} identity)
        doc @doc
        _ (if (not= (:status doc) 200) (throw (Exception. (str "Got bad status: " doc))))]
    doc
    ))

(t/ann ^:no-check net.cgrand.enlive-html/select [(HMap) (t/Vec Any) -> (HMap)])

(t/def-alias Node "Node representing an actor or film"
  (HMap :mandatory {:id String 
                    :links (t/Set String)
                    :title String}
        :optional {:d t/AnyInteger}))

(t/def-alias Graph "Graph of tokens to nodes"
  (clojure.lang.IPersistentMap String Node))

;(t/ann java.lang.String/startsWith [String -> Boolean])
(t/non-nil-return java.lang.String/startsWith :all)
(t/ann ^:no-check net.cgrand.enlive-html/html-resource [java.io.Reader -> '{}])
;(t/ann clojure.core/set [ (t/Seqable Any) -> t/Set])
(t/ann ^:no-check clojure.string/replace 
       [String
        (U String Character java.util.regex.Pattern) 
        (U String Character [(t/Seq String) -> String]) -> String ])

(t/ann ^:no-check resource->title [(HMap) -> String])
(t/tc-ignore (defn resource->title [doc]
               (-> doc
                   (as-> % (:content (first (html/select % [:title]))) )
                   (as-> % (clojure.string/replace (apply str %) #"\n" "")))))


;; Extract a list of the hrefs over a bunch of items like this:
;;     <div class="title"> 
;;        <a href="/title/tt0060438/" onClick="_gaq.push(['_trackEvent', 'Find', '', '']);">A Funny Thing Happened on the Way to the Forum</a> (1966)
;;    </div>

(t/ann ^:no-check resource->links [String (HMap) -> (t/Set String)])
(defn resource->links [id resource]
  (-> resource
      (html/select [:div.title])
      ;; Turn into list of pairs [href, title]
      (as-> doc (map (t/fn> :- (t/Seq String) [e :- (HMap)]
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
  (apply clojure.set/union :links nodes)
  )

(t/ann fetch-node-from-imdb [String -> Node])
(defn fetch-node-from-imdb
  "Attacks the mobile version of the IMDB site to pull down information about an
  actor or film, to be specified like id=/name/nm123455/ or /title/tt2343243/
  (note slashes).  Returns vector of [title [links]] where links are in the same
  format as the id.  Links are to films if the id was an actor, or to actors if the
  id was a film.  In the former case, we attempt to reap only films and not TV shows."
  [id]
   (let [urls (if  (.startsWith ^String id "/name") ; /title
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
                        {:title title :links links :id id}))
         title   (or (:title (first docs)) "Not found" )
         links   (merged-links docs)]
     {:title title :links links :id id}
))

; Restrict links for testing.
;(def allowed-links #{"/name/nm0000257/" "/name/nm0748620/" "/title/tt1655460/"})
;(defn restrict-links [links]  (filter #(allowed-links %) links))

(t/def-alias MongoRecord "Mongo key value pairs"
  (clojure.lang.IPersistentMap Any Any))

(t/ann ^:no-check assure-node [MongoRecord -> Node])
(defn assure-node [r]
  (assert string? (:id r))
  (assert string? (:name r))
  (assert seq? (:links r))
  (assert (every? string? (:links r)))
  (assoc r :links (set (:links r))))



(t/ann ^:no-check monger.collection/find-maps [String  MongoRecord -> (t/Vec MongoRecord)])
(t/ann ^:no-check monger.collection/insert-and-return  [String MongoRecord -> MongoRecord])
(t/ann fetch-node-from-mongo [String -> Node])
(defn fetch-node-from-mongo [id]
  (let [node (assure-node (or (first (mc/find-maps "nodes" {:id id}))
                              (mc/insert-and-return "nodes" (fetch-node-from-imdb id))
                              ))]
    (assure-node (assoc node :d 999999 :path (list id)))))


(t/ann fetch-node-from-graph [Graph String -> (Vector* Graph Node)])
(defn fetch-node-from-graph
  "Fetch a node from the current graph, adding it from imdb if necessary.
   Entries in the graph are id ==> {:id id :links links :title title :d dist },
   where distance is initialized to be something large.
   Returns [newgraph entry]."
  [graph id]
  (if-let [node (graph id)]
    [graph node]
    (let [node (fetch-node-from-mongo id)]
      [(assoc graph id node) node])))

(t/ann distance [Node -> t/AnyInteger])
(defn distance [node] (or (:d node) 999999999))

(t/ann assoc-in-graph [Graph (t/Vec Any) Any -> Graph])
(t/tc-ignore (defn assoc-in-graph [g sel v] (assoc-in g sel v)))

(t/ann ^:unchecked coerce-list-string [Any -> (clojure.lang.IPersistentList String)])
(t/tc-ignore (defn coerce-list-string [x] x))

(t/ann update-distance [Node Graph String -> Graph])
(defn update-distance
  "Update :d field in graph node id to be one greater than di (if that will reduce it).
Returns the updated graph."
  [entry graph id]
  (let [di (distance entry)
        [graph node] (fetch-node-from-graph graph id)
        _ (assert integer? (:d node))
        ret (if (<= (distance node) di)  graph
                (-> graph
                    (assoc-in-graph [id :d] (inc di))
                    (assoc-in-graph [id :path] (conj (coerce-list-string (:path entry)) id))))]
    ret
    ))

(t/def-alias PriorityMap clojure.data.priority_map.PersistentPriorityMap)

(t/ann set-visited [Graph -> Graph])
(t/tc-ignore (defn set-visited [g]) )

(t/ann ^:no-check update-queue [PriorityMap Graph String (t/Seqable String) -> PriorityMap])
(t/tc-ignore (defn update-queue [queue graph id links] 
               (reduce (fn [q id] (assoc-queue q id (:d (graph id)))) queue links)))

; visit node and update distances in all unvisited, connected nodes
(t/ann visit-node [Graph PriorityMap String ->
                   (Vector* Graph PriorityMap)])
(defn visit-node
  "Visits node id, and update the :d fields of its neighbors.
   queue is a priority queue of id=>distance, containing entries only for unvisited
   nodes.  We will use this to determine what to visit next, Dijkstra style."
  [graph queue id]
  (println "Visiting" id)
  (let [[graph entry] (fetch-node-from-graph graph id)
        di            (:d entry)
        links         (:links entry)
        links	      (filter #(nil? (:visited (graph %))) links)
        graph         (reduce (partial update-distance entry) graph links)
        queue         (update-queue queue graph id links)
        graph         (assoc-in-graph graph [id :visited] true)
        ]
    [graph queue]))

(t/ann find-distance [String String -> (t/Option Any)])
(t/ann ^:no-check clojure.data.priority-map/priority-map [ -> clojure.data.priority_map.PersistentPriorityMap])
(defn find-distance 
  "E.g. (find-distance \"/name/nm0000257/\" \"/name/nm0000295/\")
   Use Dijkstra algorithm to find shortest path from id to target."
  [id target]
    (let [ [graph node]  (fetch-node-from-graph {} id)
           graph         (assoc-in-graph graph [id :d] 0)
           queue         (priority-map)
]
      (t/loop> [graph :- Graph graph
                queue :-  clojure.data.priority_map.PersistentPriorityMap queue
                id    :-  String id]
        (if (= id target)
          (get graph id)  ; otherwise Cannot invoke type: (HMap :mandatory {})
          (let [[graph queue]    (visit-node graph queue id)
                closest        (peek queue)
                closest         (first closest)]
            (if (empty? queue)
                "Couldn't find a path!"
                (recur graph
                       (pop queue)
                       closest)))))))

; Useful when locked out by IMDB.
;(count (repeatedly 10 (fn [] (try (find-distance "/name/nm0641304/" "/name/nm3381950/") (catch Exception e (do (println "Pausing after exception") (Thread/sleep 60000)))))))

#_(  ; stress test the queue
 (defn enqueue [pm] (pop (reduce (fn [q p] (assoc q (first p) (second p)))
                                 pm
                                 (map-indexed (fn [a b] [a b]) (repeatedly 1000 (partial rand-int 20))))))
 (peek (nth (iterate enqueue (priority-map)) 1000)))
