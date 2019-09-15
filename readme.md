** **
# Big data platform (Spark) performance acceleration

## 1. Vision and Goals Of The Project: 

Optimize the partitioning and shuffle algorithms in Spark, to perform more efficient I/O and shuffling. There is believed to be a significant opportunity for improvement in changing the I/O patterns so that large data files are read more efficiently from disk.
* Research the current spark architecture/workflow
* Provide an analysis of an existing project highlighting areas of possible improvements
* Provide possible Spark performance acceleration strategies by optimizing the shuffle and partitioning algorithms.
* Implement a new shuffle/merge manager, and insert it into the Spark software stack.

## 2. Users/Personas Of The Project
Engineers or Data scientists working on Big Data projects that use batch processing and/or real-time processing using Spark. 
