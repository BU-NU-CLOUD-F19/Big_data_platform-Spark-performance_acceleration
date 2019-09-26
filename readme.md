** **
# Big data platform (Spark) performance acceleration

## 1. Vision and Goals Of The Project: 

Shuffling could become the scaling bottleneck when running many small tasks in multistage data analysis jobs. In certain circumstances, data is too large to fit into memory, intermediate data has to keep on disks, which makes a large amount of small random I/O requests significantly slow down the performance of spark.


Optimize the partitioning and shuffle algorithms in Spark, to perform more efficient I/O and shuffling. There is believed to be a significant opportunity for improvement in changing the I/O patterns so that large data files are read more efficiently from disk.

* Improve the efficiency of the Spark shuffle phase, better than vanilla spark.
* Decrease the number of I/O operations for the reduce phase.
* Implement the N-way merge in the shuffle phase for improving efficiency.
* Experiment over different single and multi-stage jobs.
* Analyze, using different metrics, the performance improvement over vanilla spark.

## 2. Users/Personas Of The Project
People developing Spark applications.

## 3. Scope and Features Of The Project:
### Scope:
* Provide a design architecture of Riffle proposed in the riffle paper.
* Analyze and understand the existing code of spark, especially that of the shuffle phase.
* Setup both local and cloud environments to run spark jobs.
* Setup both local and cloud environments to run spark jobs.
* Provide a detailed analysis based on metrics (speed-up, diffe



## 4. Solution Concept

![image alt text](sparkArch.png)

**Aggregate:** This starts aggregation once N map outputs are generated.

### Current Implementation: ###
The current implementation of spark has three phases: Map, shuffle and reduce. Reduce phase requires all the map outputs to start its computation. Once all map outputs are shuffled, the reduce phase fetches this as input to start its processing.

### Observation: ###
A lot of time is wasted in waiting for the map jobs to finish. Since reduce requires all the map outputs, the current implementation has that many i/o operations to do. 

### Improvements proposed: ###
As per the riffle paper, adding an N-Way merger to the shuffle phase helps improve efficiency by starting to merge map outputs the moment “N” outputs are generated, This way, the time which was previously being wasted is utilized efficiently and therefore does not contribute to additional time in merging. Hence, number of I/O operations gets reduced to M/N from M, where M denotes the number of Map outputs and N denotes the factor “N” in the N-Way merge


 Here are some references we are using at the moment to work on the solution:
 
https://haoyuzhang.org/publications/riffle-eurosys18.pdf



## 5. Acceptance criteria
Improvements during the shuffling phase:
* Fewer I/O requests in general
* Less disk I/O during shuffling phase
* Less running time

**Stretch Goals:**
* Test the service/plug-in on different categories of Spark applications and deployment environment.
* Design some simple strategies that decide when to merge shuffling overhead when not to.
* Provide explanations of why there is no general method which could apply to all datasets
* Implement improvements on data partitioning phase 

## 6. Release Planning
### Tasks: ###

* Setting up the Spark Environment (latest version)
* Learn more about Spark and Hadoop
* Finding an appropriate dataset/project to perform analysis
* Analyze and understand the existing spark code and rest API, especially for the spark shuffle phase.
* Run Spark applications and profile Spark performance before and after riffle implementation.:
* Providing detailed analysis based on metrics(speed-up, the difference in number of I/O operations) on different spark jobs(single and multi-stage).

### Timeline: ###

**16th September - 29th September:** 

Setup environment, find the existing spark code, read and summarize riffle paper, learn thoroughly about the spark architecture and their phases: map, shuffle and reduce

**30th September - 13th October:**

Understand existing spark code for the shuffle phase, complete the design architecture of Riffle, discuss ideas on how to start the implementation of N-Way merge, start the implementation of N-Way merge algorithm. 

**14th October - 27th October:**

Work on the backlog, continue implementation of N-Way merge and test the correctness of algorithm implemented so far.

**28th October - 10th November:**

Brainstorm on improvements/ fine-tuning of algorithm/strategies and possibly implement them.

**11th November - 24th November:**

Continue working on possible improvements

**25th November - 8th December:**

Make a final presentation, Focus on stretch goals

** **
