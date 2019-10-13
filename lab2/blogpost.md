# Supercomputing for Big Data - Lab 2 Blogpost

**Group 14
Rafal Pytel 
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

### Experimenting with the number of executors

### KryoSerializer and Dominant Resource Calculator

## Metric optimization
