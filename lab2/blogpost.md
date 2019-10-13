# Supercomputing for Big Data - Lab 2 Blogpost

**Group 14 <br/>
Rafal Pytel <br/>
Vasileios Serentellos**

## Sections Structure

- [Initial results](#initial-results)
- [Code optimization](#code-optimization)
    - [Removing complex UDFs](#removing-complex-udfs)
    - [Parallel writes](#parallel-writes)
- [EMR optimizations](#emr-optimizations)
    - [Garbage collector](#garbage-collector)
    - [Experimenting with the number of executors](#experimenting-with-the-number-of-executors)
    - [KryoSerializer and Dominant Resource Calculator](#kryoserializer-and-dominant-resource-calculator)
- [Metric optimization](#metric-optimization)

## Initial results

In the previous lab we were assigned with the task of designing and developing an application capable of analyzing the most common topics in the news according to the GDelt dataset. In this lab, we tried to scale this application to process not just a small part of the dataset, but to use the whole dataset consisting of terabytes of data. To meet this goal we used the AWS cloud platform, utilizing various instances for a ranging number of core nodes, which differed in terms of their internal specifications. 

Initially, we decided to test our Dataframe/Dataset implementation from the previous lab, since in our opinion it seemed quite well optimized and more promising, according to our test and extrapolation results presented in the previous report. On a starting point, we needed to apply small adjustments to deploy our implementation on the AWS EMR platform. After that, we performed tests with an increasing number of segments on a cluster consisting of 10 core nodes of type *m4.large* and 1 master of the same type. At this point, it is worth mentioning that *m4.large* instance have 2 virtual cores and 8 GB of memory for about 0.019\$/h per spot instance. As we managed to perform tests up to about 1000 segments on the setup mentioned before, the execution time started to be infeasible for the requirements of this assignment, reaching up to 1 hour. At that point, we decided to increase the number of with core nodes to 20 *m4.large* instances, which provided adequate results (less than half an hour) up to the processing of 21000 segments. To be able to process an even greater number of segments we decided to switch to the machines suggested by the manual, meaning the *c4.8xlarge* instances. This instance has 36 virtual cores and 60 GB of memory. Nonetheless, the cost of this instance is much greater compared to the *m4.large* instance, as it is about 0.3\$/h per spot instance. 

At this point, we decided to spin up the setup suggested in the manual, meaning a cluster consisting of 20 *c4.8xlarge* core nodes and a master node of the same type. Even with such powerful resources, we were not able to successfully process data without errors, because they were not utilized at their full potential. Thus, we had to set one of the properties of Spark called *maximizeResourceAllocation* to true. This Spark-specific option calculates the maximum resources of a cluster and enabled us to use them during the execution of a job, in our case processing of GDELT data. After this improvement, we were able to perform the needed processing of all data within **5 minutes and 56 seconds**, as it can be seen in the following figure.

In the following 3 figures we can observe a cpu utilization of about 50% of the available resources, while the memory usage was equal to about 130GB of the total 1.2TB and the network bandwidth was up to 16.5 GB/s. In addition, as it can be seen in the execeutors' figure, garbage collection occupies about 16% of the total task time. From these results, we noticed, that there must be still place for improvement for our application, so we organized this report to depict our optimization attempts as follows: in the [Code optimization](#code-optimization) section we present the code optimizations performed along with the obtained results showing, and in the [EMR optimizations](#emr-optimizations) section we present the EMR/Spark optimizations we experimented with to better utilize our resources. Finally, in the [Metric optimization](#metric-optimization) section, we analyze the way in which we tried to optimize our chosen metric to accommodate both run-time and the cost of execution.

## Code optimization

In this section, we present the two main improvements we decided to apply on our implementation to increase the utilization of the available resources and consequently decrease the processing time. Inspired by the material presented in the lectures we decided to remove all user-defined functions (UDFs) from our implementation, and also decrease the number of shuffling operations by applying a parallel writes concept to the output folder. 

### Removing complex UDFs

As our first code optimization, we decided to remove any unnecessary user-defined functions or replace some of them by their optimized framework versions (e.g *array_distinct* function instead of our implementation). Thanks to this step we observed a decrease in processing time to **5 minutes and 51 seconds**, which corresponds to a small improvement of 1.4%. 

### Parallel writes
The next code improvement applied concerned the application of a parallel writes scheme. We decided that instead of collecting all results to one machine and then writing them to the designated output folder, it would be faster to omit the coalescing step, so that all core machines would write their individual results to the designated folder. We achieved a processing time of **5 min and 49 seconds**, which corresponds to an improvement of 0.5%. This indicated that our initial implementation was quite well optimized and we had to look for possible improvements somewhere else. 

## EMR optimizations
Apart from the aforementioned code optimizations we also attempted to improve the performance of our big data processing cluster by fine tuning our cluster configuration, leveraging Yarn/Spark configuration flags to better meet the needs of the problem.

### Garbage collector
Initially, we noticed that the garbage collection process occupied 15.9% of the total task time, as it can be seen in Figure , which is considered a relatively high percentage of processing time. Thus, after consulting [multiple](https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/) [sources](https://umbertogriffo.gitbooks.io/apache-spark-best-practices-and-tuning/content/tuning-java-garbage-collection.html) [online](https://databricks.com/blog/2015/05/28/tuning-java-garbage-collection-for-spark-applications.html) we decided to use the Garbage First Garbage Collector (G1GC), since it is supposed to provide both high throughput and low latency overcoming in that way the limitations of the default Parallel Collector (ParallelGC). To do so, we activated the *-XX:+UseG1GC* configuration flag, and in parallel we decreased the *InitiatingHeapOccupancyPercent* option's value from 45 to 35, to let G1GC initiate garbage collection sooner, in order to avoid potential garbage collection for the total memory, which would require a significant amount of time. This strategy led to a decrease in the total processing time to **5 minutes**, as it can be seen in the following figure, while the garbage collection process now occupied solely 3% of the total task time.

In the following figures, the cpu, memory and network utilizations are presented when G1GC was used. A slight improvement to all these metrics compared to the results obtained for our best performing configuration from the [Code optimization](#code-optimization) section can be observed. In particular, the cpu utilization has increased up to 55.7%, the memory usage to 431 GB, and the network bandwidth to 17.9 GB/sec.

### Experimenting with the number of executors
In an attempt to increase the parallel computation in our cluster we decided to experiment with the number of executors per core node. To properly do that we had to split the resources of each node to accommodate the needs of the executors. We followed the advice provided by [Amazon](https://aws.amazon.com/blogs/big-data/best-practices-for-successfully-managing-memory-for-apache-spark-applications-on-amazon-emr/) and experimented with the following values:

- *spark.executor.cores*: The number of cores that each executor should use. According to Amazon, assigning executors with a large number of virtual cores would lead to a low number of executors and, therefore, reduced parallelism, while assigning a low number of virtual cores would lead to a high number of executors, thus a larger amount of I/O operations. It was suggested to use 5 cores per executor, which in our case would lead to 7 executors per instance. This value was calculated as follows:
    *number of executors per instance = (total number of virtual cores per instance - 1)/spark.executors.cores*

One virtual core is subtracted from the total number of virtual cores to be reserved for the Hadoop daemons.
- *spark.executor.memory*: The total executor memory can be calculated by dividing the total RAM per instance by the number of executors per instance, again providing 1GB for the Hadoop daemons. This total executor memory includes both the executor memory and the overhead, thus spark.executor.memory was set equal to 90% of the total executor memory.  
- *spark.yarn.executor.memoryOverhead*: This value was set to 10% of the total executor memory.
- *spark.driver.memory*: This value was set equal to spark.executor.memory.
- *spark.driver.cores*: This value was set equal to spark.executor.cores.
- *spark.executor.instances*: This value can be calculated by multiplying the number of executors with the total number of instances, again reserving one executor for the driver.
- *spark.default.parallelism*: The formula to calculate this value is the following:
    *spark.default.parallelism = 2 x spark.executor.instances x spark.executors.cores*
    
After appropriately calculating all the parameters presented above we experimented with 2,7, and 11 executors per instance. The results obtained can be seen in the following table . As it can be seen, when we used 2 executors per instance we obtained the worst results, probably because of reduced parallelism, while the best results were achieved when we increased the number of executors per instance to 7. This configuration complies with Amazon's suggestion of 5 cores per executor, yet the results obtained with the sole usage of the G1GC garbage collector prevailed. Finally, the further increase of the number of executors did not lead to a further performance increase most probably due to I/O limitations of our cluster.

Number of executors per node | Processing Time (min) | CPU utilization (%) | Memory usage (GB)| Network bandwidth (GB/s) |
 :-----: | :--------------: | :------: | :------: | :---------:
   2   |   7 min 13 s   |   34   |   200  |   13.5          
   7   |   5 min 32 s   |   59   |   400  |   17.8          
  11   |   5 min 52 s   |   57   |   300  |   16.4          


To reduce the aforementioned possible I/O bottleneck we used a 80GBit Root Device Volume size, which did not improve significantly the processing time (still 5.5 minutes), yet it boosted the CPU utilization to 65% (unfortunately due to "budget limitations we cannot provide graphs of this run). In the following figures the graphs for each of the aforementioned runs can be found. 

### KryoSerializer and Dominant Resource Calculator
Finally, in an attempt to further improve the performance of our system, we specifically set the *spark.serializer* flag to *org.apache.spark.serializer.KryoSerializer*, so that the Kryo Serializer could be used. Yet, we did not obtain any significant improvement neither in the execution time nor in the rest Ganglia metrics, which could be explained by the fact that Spark Datasets use specialized Encoders, which "understand" the internal structure of the data and can efficiently transform objects into internal binary storage, rather than standard serializers.

In addition, in a final attempt to leverage even more the capabilities of our instances we replaced the *DefaultResourceCalculator* YARN Capacity Scheduler with the *DominantResourceCalculator* one, which takes into account both the memory and cpu availability in the resource allocation process. Again no significant improvement was observed. 

## Metric optimization
As it was mentioned in the report of the first lab assignment, we decided that in the metric to optimize we should take into account both the performance of the system and the cost of the machines used. As a result, we decided to maximize the *throughput / cost* ratio, where *throughput = dataset size / execution time* and *cost = number of instances x price per hour x execution time / 60*

To do so, we varied the number of core nodes used so that the price would decrease, at the expense of performance degradation of course. The configurations that provided the best results so far were tested, meaning 7 executors per node with the Garbage First Garbage Collector (G1GC) applied, for a varying number of core nodes, ranging from 10 to 20 with a step of 5. We attempted to retain the processing time lower or close to 10 minutes, so that the execution time could be still considered adequate. The results of our experimentation can be seen in the following table.

Number of Core Nodes | Processing Time (min) | Dataset size (TB) | Price/hour per node ($/h)| Total cost ($) | Our metric 
:------: | :-------: | :------: | :--------: | :-----------: | :----------:
   10    |     10     |         |            |     0.545     |   13.762
   15    |     6.9    |  4.5    |   0.297    |     0.547     |   19.871
   20    |     5.5    |         |            |     0.572     |   23.839
         
At this point it should be mentioned that for each configuration the appropriate setting of the all parameters was calculated as it was explained in the [Experimenting with the number of executors](#experimenting-with-the-number-of-executors) section. It can be easily seen from the results presented in the table above that the changes in the total cost of each configuration are quite small, especially between the 15-nodes and 10-nodes configurations, while the difference in processing time between the 15-nodes and 20-nodes configurations is less than 1.5 minute. Yet, as our metric suggests the qualifying option would be the 20-nodes configuration, since this one provides the higher value for our metric.
