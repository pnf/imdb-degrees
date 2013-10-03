(defproject imdb "0.1.0-SNAPSHOT"
  :description "FIXME: write description"
  :url "http://example.com/FIXME"
  :license {:name "Eclipse Public License"
            :url "http://www.eclipse.org/legal/epl-v10.html"}
  :repositories {"sonatype-oss-public" "https://oss.sonatype.org/content/groups/public/"}

  :dependencies [[org.clojure/clojure "1.5.1"]
                 [clj-stacktrace "0.2.5"]
                 [http-kit "2.1.10"]
                 [enlive "1.1.4"]
                 ;[org.clojure/core.typed "0.2.6"]
                 [org.clojure/core.typed "0.2.13"]
                 [org.clojure/core.async "0.1.242.0-44b1e3-alpha"]
                 [com.novemberain/monger "1.5.0"]
                 [org.clojure/data.priority-map "0.0.2"]]

  :plugins [[lein-typed "0.3.0"]]
  :core.typed {:check [imdb.core]}  ; "lein typed check"


  :jvm-opts ^:replace ["-Xmx1g" "-server"]
)

