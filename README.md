# Parallel n-gram generation with Hadoop

In this work we will show how to implement the generation of n-grams in a parallel and distributed way using the Hadoop framework and what optimizations can be used to further improve our approach.

## Usage
In order to run the program with ```main.java```, there must be a local Hadoop installation running. The program takes exactly 7 arguments. In order, they are:

1. HDFS input directory
2. HDFS output directory of the first MapReduce job
3. HDFS output directory of the second MapReduce job
4. ```n```, as the n in *n*-gram for letters
5. ```m```, as the n in *n*-gram for words
6. ```true``` or ```false```, to choose whether or not to remove the stopwords from the books
7. the local hadoop installation directory

So, some sample execution arguments can be:
```
hdfs://localhost:9000/input hdfs://localhost:9000/output1 hdfs://localhost:9000/output2 4 2 true /home/user/hadoop
```
