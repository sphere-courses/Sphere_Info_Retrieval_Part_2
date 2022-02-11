#!/usr/bin/env bash

# extrac links from sites and encode them in indices
hadoop jar build/libs/hw04-1.0-SNAPSHOT.jar GenerateURLSJob /data/infopoisk/hits_pagerank/docs-*.txt real_urls /data/infopoisk/hits_pagerank/urls.txt
hadoop fs -mv real_urls/part-r-00000 real_urls/real_urls.txt

# build graph from links
 hadoop jar build/libs/hw04-1.0-SNAPSHOT.jar ExtractLinksJob /data/infopoisk/hits_pagerank/docs-*.txt extracted_graph real_urls/real_urls.txt

