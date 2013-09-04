(ns imdb.core
  (:require [clojure.data.priority-map :refer [priority-map]]
             [org.httpkit.client :as http]
             [net.cgrand.enlive-html :as html]
))



(defn fetch-node-from-imdb*
  "Attacks the mobile version of the IMDB site to pull down information about an
  actor or film, to be specified like id=/name/nm123455/ or /title/tt2343243/
  (note slashes).  Returns vector of [title [links]] where links are in the same
  format as the id.  Links are to films if the id was an actor, or to actors if the
  id was a film.  In the former case, we attempt to reap only films and not TV shows."
  [id]
   (let [url (if  (.startsWith id "/name") ; /title
               (str "http://m.imdb.com" id "filmotype/actor")
               (str "http://m.imdb.com" id "fullcredits/cast"))
         _ (println "Fetching" url)
         doc (http/request {:url url :method :get} identity)
           (if (not= (:status @doc) 200) (throw (Exception. (str "Got bad status: " @doc))))
         doc (html/html-resource (java.io.StringReader. (:body @doc)))
         title  (:content  (first (html/select doc [:title])))
         doc (html/select doc [:div.title])
         doc (map (fn [e]
                    [(-> e :content second :attrs :href)
                     (-> e :content last)]) doc)
         doc (if (.startsWith id "/name") (filter #(re-find #"\(\d\d\d\d\)" (last %)) doc) doc)
         _ (println "Fetched" title)
         ]
     [title (map first doc)]
     ))

(def fetch-node-from-imdb (memoize fetch-node-from-imdb*))

(defn fetch-node-from-graph
  "Fetch a node from the current graph, adding it from imdb if necessary.
   Entries in the graph are id ==> {:id id :links links :title title :name name :d dist },
   where distance is initialized to be something large.
   Returns [newgraph entry]."
  [graph id]
  (let [ret (graph id)
        ret (if ret [graph ret]
                (let [[title links] (fetch-node-from-imdb id)
                      ret {:id id :links links :title title :name name :d 99999999 }]
                  [(assoc graph id ret) ret]))]
    ret))

; return updated graph
(defn update-distance
  "Update :d field in graph node id to be one greater than di (if that will reduce it).
Returns the updated graph."
  [di graph id]
  (let [[graph node] (fetch-node-from-graph graph id)
        ret (if (< (:d node) di)  graph
          (assoc-in graph [id :d] (inc di)))
        ]
    ret
    ))

; visit node and update distances in all unvisited, connected nodes
(defn visit-node
  "Visits node id, and update the :d fields of its neighbors.
   queue is a priority queue of id=>distance, containing entries only for unvisited
   nodes.  We will use this to determine what to visit next, Dijkstra style."
  [graph queue id]
  (println "Visiting" id queue)
  (let [[graph entry] (fetch-node-from-graph graph id)
        di            (:d entry)
        links         (:links entry)
        links	      (filter #(nil? (:visited (graph %))) links)
        graph         (reduce (partial update-distance di) graph links)
        ; _ (println "About to reduce queue")
        queue         (reduce (fn [q id] (assoc q id (:d (graph id)))) queue links)
        graph         (assoc-in graph [id :visited] true)
        ]
    [graph queue]))

(defn find-distance 
  "E.g. (find-distance \"/name/nm0000257/\" \"/name/nm0000295/\")
   Use Dijkstra algorithm to find shortest path from id to target."
  [id target]
    (let [ [graph node]  (fetch-node-from-graph {} id)
           graph         (assoc-in graph [id :d] 0)
          ;queue         (priority-map)
           queue         {}]
      (loop [graph  graph
             queue  queue
             id     id]
        (if (= id target)
          (graph id)
          (let [[graph queue]    (visit-node graph queue id)
                ;closest-req      (peek queue)
                ; Absolutely hideous brute force search for closest node.
                closest         (reduce (fn ([a b] (if (< (second a) (second b)) a b))) queue)
                _  (println "Closest was" closest)
                closest         (first closest)
                ;queue           (pop queue)
                queue           (dissoc queue closest)
                ]
            (if (empty? queue)
                ;(nil? closest-req)
                "Couldn't find it"
                (recur graph queue closest)))))))

; 
