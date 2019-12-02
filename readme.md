** **
# Big data platform (Spark) performance acceleration

## 1. Vision and Goals Of The Project: 

Shuffling could become the scaling bottleneck when running many small tasks in multistage data analysis jobs. In certain circumstances, data is too large to fit into memory, intermediate data has to keep on disks, which makes a large amount of small random I/O requests significantly slow down the performance of spark.


Optimize the partitioning and shuffle algorithms in Spark, to perform more efficient I/O and shuffling. There is believed to be a significant opportunity for improvement in changing the I/O patterns so that large data files are read more efficiently from disk.

* Improve the efficiency of the Spark shuffle phase, better than vanilla spark.
* Decrease the number of I/O operations for the reduce phase.
* Implement the N-way merge in the shuffle phase for improving efficiency.
* Analyze, using different metrics, the performance improvement over vanilla spark.

## 2. Users/Personas Of The Project
People developing Spark applications.

## 3. Scope and Features Of The Project:
### Scope:
* Provide a design architecture of Riffle proposed in the riffle paper.
* Analyze and understand the existing code of spark, especially that of the shuffle phase.
* Provide a detailed analysis based on metrics (speed-up, difference in number of disk i/o operations) to compare the performances of the job run before and after riffle implementation.


## 4. Solution Concept

![image alt text](sparkArch.png)

**Aggregate:** This starts aggregation once N map outputs are generated.

### Current Implementation: ###
The current implementation of spark has three phases: Map, shuffle and reduce. Reduce phase requires all the map outputs to start its computation. Once all map outputs are shuffled, the reduce phase fetches this as input to start its processing. The processing can often scale well by splitting jobs into smaller tasks for better parallelism.

### Observation: ###
 All-to-all data transfer, Shuffle Operations become the scaling bottleneck when running many small tasks in multi-stage data analytics jobs. The key observation is that this bottleneck is due to the superlinear increase in disk I/O operations as data volume increases. This is due to the fact that the number of shuffle I/O requests between map and reduce stages grows quadratically as the number of tasks grows, and the average size per request actually shrinks linearly.

### Improvements proposed: ###
As per the riffle paper, adding an N-Way merger to the shuffle phase helps improve efficiency by merging small intermediate  shuffle map outputs files into larger blocks. We will merge the map outputs as soon as the “N” outputs are generated. Hence, number of I/O operations gets reduced to M/N from M, where M denotes the number of Map outputs and N denotes the factor “N” in the N-Way merge

![image alt text](Riffle.JPG)

 Here are some reference we are using at the moment to work on the solution:
 
https://haoyuzhang.org/publications/riffle-eurosys18.pdf


## 5. Acceptance criteria

* Implement Riffle’s N-Way merge algorithm.
* Prove a decrease in the number of I/O operations by the reduce phase.
* Prove speed-up in the total execution time of the spark job before and after implementation of Riffle.

**Stretch Goals:**

* Analyze the difference between different disks/file systems on AWS.
* Find the relationship between N and file attributes(number, length, type of spark job).
* Finding better storage techniques that improve in-memory storage capacity.
* Implementing merge policy to choose between N-Way merge and fixed-size block merge.

## 6. Release Planning
### Tasks: ###

* Setting up the Spark Environment (latest version)
* Learn more about Spark Architecture(Map, shuffle and Reduce phase).
* Finding an appropriate dataset/project to perform analysis.
* Analyze and understand the existing spark code and rest API, especially for the spark shuffle phase.
* Run Spark applications and profile Spark performance before and after riffle implementation.
* Providing detailed analysis based on metrics(speed-up, the difference in number of I/O operations) on different spark jobs(single and multi-stage).

### Timeline: ###

**16th September - 29th September:** 

Setup environment, find the existing spark code, read and summarize riffle paper, learn thoroughly about the spark architecture and their phases: map, shuffle and reduce

Presentation Link: https://1drv.ms/p/s!Aj2G3numQP0utHXYOnMyJf1dvMkP

**30th September - 13th October:**

Understand existing spark code for the shuffle phase, complete the design architecture of Riffle, discuss ideas on how to start the implementation of N-Way merge,find the appropriate data set, start the implementation of N-Way merge algorithm.

Presentation Link: https://1drv.ms/p/s!Aj2G3numQP0utHidtUcEnSf759UO 

Paper Presentation Link: https://1drv.ms/p/s!Aj2G3numQP0utHotq5uGxGZmb2EV

**14th October - 27th October:**

Work on the backlog, continue implementation of N-Way merge and test the correctness of algorithm implemented so far.
Presentation Link: https://1drv.ms/p/s!Aj2G3numQP0utHziF9GZFdaXCy_h

**28th October - 10th November:**

Brainstorm on improvements/ fine-tuning of algorithm/strategies and possibly implement them.
Presentation Link: https://1drv.ms/p/s!Aj2G3numQP0utQGpueCx88jb73_i

**11th November - 24th November:**

Continue working on possible improvements
Presentation Link: https://1drv.ms/p/s!Aj2G3numQP0utQP9S0yfT6hds_E7

**25th November - 8th December:**

Make a final presentation, Focus on stretch goals

** **
