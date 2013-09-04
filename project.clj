(defproject imdb "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :dependencies [[org.clojure/clojure "1.5.1"]
                 [http-kit "2.1.10"]
                 [enlive "1.1.4"]
                 [com.novemberain/monger "1.5.0"]
                 [org.clojure/data.priority-map "0.0.2"]]

  :jvm-opts ^:replace ["-Xmx1g" "-server"]
)

