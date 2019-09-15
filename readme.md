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

## 3. Scope and Features Of The Project:
### Scope:
* Researching the existing solutions/papers to identify the best approach.
* Find a dataset and upload it to AWS S3.
* Run an existing spark project on the AWS EC2 cluster and analyze run times with different sizes of the dataset.
* Use spark history server and other tools to analyze stages, I/O operations and amount of shuffled data, to analyze what can be the areas for improvement.
* Provide improved speed-up metrics.
* (Optional) Possible improvements using caching of meta-data.

### Features:
* **Onboarding**: Designing a service that is easy to onboard
* **User-friendly**: A service that is easy to understand and use
* **Availability**: It will be accessible in all regions
* **Scalability**: Can scale to a large number of users, projects, and services
* **Confidentiality**: It will be a completely abstracted service.
* **Fault-tolerant**: Final output will not be changed compared with original applications, and the service will be resilient


