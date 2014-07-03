pig-hyperloglog
===============

Several [Apache Pig](http://pig.apache.org/) user defined functions (UDFs) to compute and use the HyperLogLog algorithm.  
  
Other implementations exist (for example, [this one](http://datafu.incubator.apache.org/docs/datafu/guide/estimation.html)). This project was implemented to complement [the hyperloglog mysql plugin](https://github.com/amirtuval/mysql-hyperloglog) and uses the exact same implementation. Thus, it enables you to compute a HLL string in a pig script, import the results into MySQL, and then invoke the MySQL HLL functions on the data to analyze the data and get cardinality estimation.
