# 132b_hadoop_wikipedia


### Running MapReduce Program on Cluster

* In the Driver class, change the output path (args[2]) and input path(args[1]) (see comments in Driver class)
* Export the program to a Runnable JAR file
* scp the jar from local machine to Diadem (department server)
* scp the jar from Diadem to XCN
* running jar on cluster
$ yarn jar myJar.jar Driver /data/wiki_csv ./output_folder_name

### Useful links

[http://makble.com/what-is-term-vector-in-lucene](http://makble.com/what-is-term-vector-in-lucene)

[http://makble.com/what-is-lucene-term](http://makble.com/what-is-lucene-term)

[http://makble.com/how-to-do-lucene-search-highlight-example](http://makble.com/how-to-do-lucene-search-highlight-example)
