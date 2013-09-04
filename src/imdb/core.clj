(ns imdb.core
  (:require [clojure.data.priority-map :refer [priority-map]]
             [org.httpkit.client :as http]
             [net.cgrand.enlive-html :as html]
))



(defn fetch-node-from-imdb [id]
  "id=/name/nm123455/ or /title/tt2343243"
  (identity (let [url (if  (.startsWith id "/name") ; /title
                       (str "http://m.imdb.com" id "filmotype/actor")
                       (str "http://m.imdb.com" id "fullcredits/cast"))
                  _ (println "Fetching" url)
                 doc (http/request {:url url :method :get} identity)
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
             )))

(defn fetch-node-from-graph [graph id]
  (let [ret (graph id)
        ret (if ret [graph ret]
                (let [[title links] (fetch-node-from-imdb id)
                      ret {:id id :links links :title title :name name :d 99999999 }]
                  [(assoc graph id ret) ret]))]
    ret))

; return updated graph
(defn update-distance [di graph id]
  (let [[graph node] (fetch-node-from-graph graph id)
        ret (if (< (:d node) di)  graph
          (assoc-in graph [id :d] (inc di)))
        ]
    ret
    ))

; visit node and update distances in all unvisited, connected nodes
(defn visit-node [graph queue id]
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

(defn find-distance [id target]
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

; (find-distance "/name/nm0000257/" "/name/nm0000295/")
