# **Reddit-Trend-Bitcoin** 

The purpose of Reddit-Trend-Bitcoin is to analyze how the comments of Reddit cryptocurrency community could potentially affect others thought and as a result influence the price of Bitcoin. 

[Demo](https://www.youtube.com/watch?v=u8knZziJE6o&feature=emb_logo) and [Slide](https://docs.google.com/presentation/d/1YPG49iJSVNnVeLwXt1wXepNnuA7-XJpmp-iD18YTBms/edit).

![](https://github.com/gary19941216/bitcoin-fluc-detector/blob/master/Images/Bitcoin-Reddit-Historical.png)

## **Table of Contents** 
  1. [Problem Statement](#problem-statement)
  2. [Data Pipeline](#data-pipeline)
  3. [Tool Setup](#tool-setup)
  4. [Data source](#data-source)
  5. [Repo directory structure](#repo-directory-structure)

## **Problem Statement** 
Bitcoin attracted more and more investors globally in the last decades, along with the fact that its prices have fluctuated dramatically. 
Even though many factors might involve in these fluctuations, it is still valuable to figure out a potential pattern that shows how the prices impacted by one or more variables. We know that if the amount of investors who were holding a positive attitude is large enough, the prices of bitcoin may thus increase since many of them would possibly buy in bitcoin, and vice versa. 

Interestingly, there are a few online communities with large numbers of users actively sharing their insights and predictions about the market. 
These comments might influence people's expectations of bitcoin's prices and their investment decisions. 

In this project, several subreddits, such as "Bitcoin" and "Cryptocurrency", would be analyzed and compared with the trends of bitcoin's prices in the past decades. 
Its purpose is to see if there is any significant correlation between the users' comments and bitcoin prices. 

## **Data Pipeline** 

The project is composed of two main parts, a batch and a real-time pipeline, along with the unify process for historical and real-time data.

### **Batch Pipeline** 

The upper part is the batch pipeline which serves as the ETL pipeline. The raw data is stored in Amazon S3 as a collection of files in the JSON format. By using Apache Spark, I extracted the information from Reddit's comments that were relevant to bitcoin prices and then accumulated scores (difference between the numbers of upvotes and downvotes) within different time intervals. In the analysis part, firstly, indentified spikes of Reddit comments scores and bitcoin prices by using Spark Window Functions. Second, I joined the spikes of the two datasets if the spikes of the Reddit comments scores appeared one day before that of bitcoin prices. This process excluded the situation that bitcoin prices invoked the spike of Reddit comments and ensured the logic validity of the results. 

### **Real-Time Pipeline** 

The lower part is the real-time pipeline which is architected using Apache Kafka as the backbone. Real-time data would be produced to Kafka topic and consumed using Spark structured streaming.

### **Unify Process** 

At last, Apache Airflow is applied to unify historical data and real-time data which was stored in Cassandra.

![](https://github.com/gary19941216/bitcoin-fluc-detector/blob/master/Images/data%20pipeline.png)


## **Tool Setup** 

  Specific details for tool setup and usage are under each folder

  1. [Spark](https://github.com/gary19941216/bitcoin-fluc-detector/tree/master/spark)
  2. [Kafka](https://github.com/gary19941216/bitcoin-fluc-detector/tree/master/kafka/kafka_2.12-2.5.0)
  3. [Airflow](https://github.com/gary19941216/bitcoin-fluc-detector/tree/master/airflow)
  4. [Cassandra](https://github.com/gary19941216/bitcoin-fluc-detector/tree/master/cassandra)
  5. [Dash](https://github.com/gary19941216/bitcoin-fluc-detector/tree/master/dashboard)

## **Data source** 
[Reddit comments](https://files.pushshift.io/reddit/comments/) data from 2006 to 2019 (4.9TB uncompressed)

[Bitcoin price](http://api.bitcoincharts.com/v1/csv/) data (10GB)

## **Repo directory structure**

```bash
├── README.md
├── dashboard
│   └── frontend.py
├── cassandra
│   ├── cassandra.yaml
│   └── cassandra-rackdc.properties
├── airflow
│   └── sparkDAG.py
├── kafka
│   └── kafka_2.12-2.5.0
│       ├── config
│       │   ├── server-1.properties
│       │   ├── server-2.properties
│       │   ├── server-3.properties
│       │   └── zookeeper.properties
│       └── producer
│           ├── pom.xml
│           ├── src
│               └── main
│                   └── java
│                       └── apps
│                           ├── bitcoinProducer.java
│                           └── redditProducer.java        
└── spark
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
            






