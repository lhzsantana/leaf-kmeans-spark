
# Grouping Field areas in Leaf operations response using K-means with Apache Spark

This repository is an example showing how we can use Leaf API's to group areas accondingly to the properties return by the machinery using K-means algorithm present in Spark ML.

- Leaf Operations API: https://leaf-agriculture.github.io/docs/docs/operations_overview
- Spark documentation: https://spark.apache.org/docs/0.9.1/java-programming-guide.html

# Steps

The code will performe the following steps:

1. Connect to Leaf API
2. Will query for all the files in the operations endpoint
3. Will iterate over the list of files and for each standard GeoJson, it will:
3.1. Calculate the k-means (https://en.wikipedia.org/wiki/K-means_clustering) using the Apache Spark
