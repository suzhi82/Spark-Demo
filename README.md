# Spark-Demo

## Main files
Main program: `/src/main/scala/com/abc/spark/AnonDemo.scala`  
Test unit:    `/src/main/scala/com/abc/spark/AnonDemoTest.scala`


## How big files can be processed?
In theory, the program can handle data files larger than 2GB.

It depends on the available space of disks, memory, and CPU cores in the distributed cluster. 

According to the source code, we know that `Spark` uses the slicing method of `hadoop 1.X`.

Its core formula is `Math.max(minSize, Math.min(goalSize, blockSize))`.

The `minSize` is fixed at `1`. 

The `blockSize` is determined by the configuration `dfs.block.size` in the cluster, and the default value is `128MB`.

The `goalSize` is equal to `totalSize / (numSplits == 0? 1: numSplits)`, `totalSize` is the total size of the data files, and `numSplit` is the minimum number of partitions.

In other words, Spark's slice size depends on which is smaller, `blockSize` or `goalSize`.

When Spark's `Driver` cuts the file into slices, it will decompose the logic of `RDD` processing into tasks and give them to each `Executor` to run, so that if there are enough resources to create containers running Driver and Executors, any data set should be handled.

Meanwhile, in order to save disk space, we can use a fast compression format that supports slicing, such as `lzo`.


