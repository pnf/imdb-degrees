# imdb

Attempt to find degrees of separation of two actors in imdb, eg:
~~~
(find-distance "/name/nm0641304/" "/name/nm3381950/")
~~~

Points of note:

* Scrape the IMDB mobile interface, which is much easier to parse.  Even still, it's not very pretty.
* Among other things, there are different URLs for actors and actresses
* For actors/accesses, extract a iist of ```/title/tt1234/``` style links to movies.  Try to exclude non-movie appearances.
* For movies, extract a list of ```/name/nm1234` style links to names.
* The graph is stored in a map keyed by either names or titles, pointing to lists of links to titles or names, respectively.
* Cache the records in a Mongo running on 3333
* The search algorithm is Dijkstra, as you'd expect.
* In addition to updating distances, we also keep a running path, which might be amusing.


## License

Copyright © 2013 FIXME

Distributed under the Eclipse Public License, the same as Clojure.
