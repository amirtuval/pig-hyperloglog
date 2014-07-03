pig-hyperloglog
===============

Several [Apache Pig](http://pig.apache.org/) user defined functions (UDFs) to compute and use the HyperLogLog algorithm.  
  
Other implementations exist (for example, [this one](http://datafu.incubator.apache.org/docs/datafu/guide/estimation.html)). This project was implemented to complement [the hyperloglog mysql plugin](https://github.com/amirtuval/mysql-hyperloglog) and uses the exact same implementation. Thus, it enables you to compute a HLL string in a pig script, import the results into MySQL, and then invoke the MySQL HLL functions on the data to analyze the data and get cardinality estimation.

Usage
=====

Four separate UDFs exist -  
HLL_CREATE, HLL_COMPUTE, HLL_MERGE, HLL_MERGE_COMPUTE.  
These are exactly the same functions as in [the hyperloglog mysql plugin](https://github.com/amirtuval/mysql-hyperloglog), so check out its documentation.  
You can also see the [UdfTest.java](src/test/java/com/amirtuval/pighll/udf/UdfTest.java) for examples.  
  
**Note:** When used from Apache pig, you need to register the project jar file, but also make sure that the libpighll.so file (or DLL on windows) can be found in the java library path.

What if I do not use pig
========================

[The HyperLogLog class](src/main/java/com/amirtuval/pighll/HyperLogLog.java) is a java class the wraps the underlying c++ implementation.  
It can be used from Hadoop map-reduce, Hive, HBase or any other JVM based program.
