# **Reddit-Trend-Bitcoin** 

[Slide](https://docs.google.com/presentation/d/1YPG49iJSVNnVeLwXt1wXepNnuA7-XJpmp-iD18YTBms/edit)

![](https://github.com/gary19941216/bitcoin-fluc-detector/blob/master/Images/Bitcoin-Reddit-Historical.png)

## **Table of Contents** 
  1. [Introduction](#introduction)
  2. [Problem Statement](#problem-statement)
  3. [Data Pipeline](#data-pipeline)
  4. [Tool Setup](#tool-setup)
  5. [Data source](#data-source)
  6. [Repo directory structure](#repo-directory-structure)

## **Introduction**
The purpose of Reddit-Trend-Bitcoin is to analyze how Reddit cryptocurrency community users' comments could possibly affect others thought and as a result influence the price of Bitcoin. 

## **Problem Statement** 
Bitcoin price fluctuates dramatically, it would be invaluable to find an insight on what could possibly influence the price.
The price of bitcoin goes up when people buys in bitcoin, and vice versa. When there are breaking news or lots of people discussing 
the price might fall or rise, susceptible people would have high possibility following the crowd and make their decision accordingly. If there are just a few people affected, the price probably wouldn't change a lot. However, Reddit "Bitcoin" and "Crypocurrency" subreddit have over one million members, every influential comments post by users could greatly affect lots of people and price could possibly rise or fall correspondingly.

## **Data Pipeline** 
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
    ├── cassandra.yaml
    └── cassandra-rackdc.properties
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
            






