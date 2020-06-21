## **How to use Spark NLP**

  1.  create new folder and go to the new folder:
  
      run command:
      
      ```sudo mkdir /usr/local/spark_nlp``` and ```cd /usr/local/spark_nlp```
      
  2.  download sentiment analysis pretrained pipeline:
  
      run command:
      
      ```wget https://s3.amazonaws.com/auxdata.johnsnowlabs.com/public/models/analyze_sentiment_en_2.4.0_2.4_1580483464667.zip```
      
  3.  install unzip:
  
      run command:
      
      ```sudo apt-get install unzip```
      
  4.  unzip ```analyze_sentiment_en_2.4.0_2.4_1580483464667.zip```:
  
      run command: 
      
      ```unzip analyze_sentiment_en_2.4.0_2.4_1580483464667.zip```
      
  5.  repeat all the 4 steps on all master and worker nodes.
  
  

## **How to run Spark job**

  1.  Compile scala code: 
  
      run ```sbt package``` in ```spark``` folder (```bit_fluc_2.11-1.0.jar``` will be created in ```spark/target/scala-2.11``` folder)

  2.  ETL for historical data:
  
      under ```bitcoin-fluc-detector``` folder, run command:
  
      ```$SPARK_HOME/bin/spark-submit --class etl.ETL --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.0,com.johnsnowlabs.nlp:spark-nlp_2.11:2.4.2 spark/target/scala-2.11/bit_fluc_2.11-1.0.jar```
      
  3.  Spark structured streaming for streaming data:
  
      under ```bitcoin-fluc-detector``` folder, run command:
  
      ```$SPARK_HOME/bin/spark-submit --class streaming.Streaming --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.0,com.johnsnowlabs.nlp:spark-nlp_2.11:2.4.2,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 spark/target/scala-2.11/bit_fluc_2.11-1.0.jar```
      
  4.  Unify historical data and real-time data:
  
      under ```bitcoin-fluc-detector``` folder, run command:
  
      ```$SPARK_HOME/bin/spark-submit --class unify.Unify --packages com.datastax.spark:spark-cassandra-connector_2.11:2.5.0,com.johnsnowlabs.nlp:spark-nlp_2.11:2.4.2,org.apache.spark:spark-sql-kafka-0-10_2.11:2.4.3 spark/target/scala-2.11/bit_fluc_2.11-1.0.jar```
