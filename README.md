# **Reddit-Trend-Bitcoin** 

[Slide](https://docs.google.com/presentation/d/1YPG49iJSVNnVeLwXt1wXepNnuA7-XJpmp-iD18YTBms/edit)

## **Table of Contents** 
  1. [Problem Statement](#problem-statement)
  2. [Data Pipeline](#data-pipeline)
  3. [Data source](#data-source)
  4. [Repo directory structure](#repo-directory-structure)

## **Problem Statement** 
Bitcoin price fluctuates dramatically, it would be invaluable to find an insight into how Reddit cryptocurrency community users might make an impact on the price.


## **Data Pipeline** 
![](https://github.com/gary19941216/bitcoin-fluc-detector/blob/master/Images/data%20pipeline.png)

## **Data source** 
[Reddit comments](https://files.pushshift.io/reddit/comments/) data from 2006 to 2019 (4.9TB uncompressed)

[Bitcoin price](http://api.bitcoincharts.com/v1/csv/) data (10GB)

## **Repo directory structure**

```bash
├── README.md
├── dashboard
│   └── frontend.py
├── Airflow
│   └── sparkDAG.py
├── Kafka
│   └── producer
│       ├── pom.xml
│       ├── src
│           └── main
│               └── java
│                   └── apps
│                       ├── bitcoinProducer.java
│                       └── redditProducer.java        
└── Spark
    ├── built.sbt
    ├── src
    │   └── main
    │       └── scala
    │           ├── etl.scala
    │           ├── streaming.scala
    │           ├── unify.scala
    │           ├── dataloader.scala
    │           ├── preprocessor.scala
    │           ├── transform.scala
    │           └── dbconnector.scala
    └── target
        └── scala-2.11
            └── bit_fluc_2.11-1.0.jar
```
            






