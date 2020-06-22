## **How to Install and Run Apache Kafka on a single Ubuntu EC2 instance**

  1.  Update apt: 
  
      ```sudo apt update```
      
  2.  Install OpenJDK package:
  
      ```sudo apt install openjdk-8-jdk```
        
  3.  Verify the Java installation:
  
      ```java -version```
      
  4.  Download Kafka:
  
      ```wget http://apache.mirrors.hoobly.com/kafka/2.5.0/kafka_2.12-2.5.0.tgz```
      
  5.  Uncompress downloaded file:
  
      ```tar -xzf kafka_2.12-2.5.0.tgz```
      
  6.  Go to the kafka folder:
  
      ```cd kafka_2.12-2.5.0```
      
  7.  Run zookeeper:
  
      ```bin/zookeeper-server-start.sh config/zookeeper.properties```
      
  8.  Open a new terminal, go to the kafka folder, run server:
  
      ```bin/kafka-server-start.sh config/server.properties```
      
  9.  Open a new terminal, go to the kafka folder, create topic:
  
      ```bin/kafka-topics.sh --create --bootstrap-server localhost:9092 --replication-factor 1 --partitions 1 --topic test```
      
  10. Verify the topic is created:
  
      ```bin/kafka-topics.sh --list --bootstrap-server localhost:9092```
      
      the terminal will show the topic you just created
      
  11. Produce message to Kafka topic you just created:
  
      ```bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test```
      
      type anyword and press enter
      
  12. Consume message from Kafka topic you just created:
  
      ```bin/kafka-console-consumer.sh --bootstrap-server localhost:9092 --topic test --from-beginning```
      
      the terminal will print the message you just typed
      
      
      
## **How to setup and run Apache Kafka Cluster**

  1.  Suppose we have three EC2 instances, we will run zookeeper and server on all the three instances.
  
      10.0.0.7 (first)
      
      10.0.0.13 (second)
      
      10.0.0.10 (third)
      
      the order matters later when configuring files.
  
  2.  configure ```zookeeper.properties```:
  
      ```vim config/zookeeper.properties```
  
      ```bash
      tickTime=2000
      # the directory where the snapshot is stored.
      dataDir=zookeeper
      # the port at which the clients will connect
      clientPort=2181
      # disable the per-ip limit on the number of connections since this is a non-production config
      maxClientCnxns=60
      # Disable the adminserver by default to avoid port conflicts.
      # Set the port to something non-conflicting if choosing to enable this
      admin.enableServer=false

      initLimit=10
      syncLimit=5
      server.1=10.0.0.7:2888:3888
      server.2=10.0.0.13:2888:3888
      server.3=10.0.0.10:2888:3888
      ```
      
      Change each server ip to your own ip, make sure all zookeeper.properties on every instances are same.
      
      dataDir=zookeeper will determined where your zookeeper folder will be created. 
      
      If you use the original dataDir=/tmp/zookeeper, it will be stored under /tmp/zookeeper.
      
      
  3.  Go to the zookeeper folder:
  
      ```cd zookeeper``` or ```cd /tmp/zookeeper``` depends on how you configure zookeeper.properties.
      
  4.  Create new file ```myid```:
  
      ```touch myid```
      
  5.  Add id into the file:
  
      ```vim myid```
      
      type 1 for the first instance, type 2 for the second instance, type 3 for the third instance.
      
  6.  Run zookeeper:
  
      ```bin/zookeeper-server-start.sh config/zookeeper.properties```
      
      Run this command on all the three instances.
      
      There will be some error when you just start the first one.
      
      It's fine. It's because the first one can't find the other two instance to vote yet.
      
      After you start zookeeper on all the three instances it will work fine.
      
  7.  Configure ```server.properties```:
  
      Open up new terminal for all three instances, and keep zookeeper running.
  
      ```vim config/server.properties```
      
      ```bash
      #same as the number in myid file in zookeeper folder, change it accordingly for every instances.
      broker.id = 1
      
      #change 10.0.0.7 to the ip for each instance
      listeners=PLAINTEXT://10.0.0.7:9092 
      
      #change the three ips to your own ip
      zookeeper.connect=10.0.0.10:2181,10.0.0.13:2181,10.0.0.7:2181  
      
      #place to store your kafka-logs, you can decide where to store on your preference.
      log.dirs=kafka-logs
      ```
      
  8.  Run server:
  
      ```bin/kafka-server-start.sh config/server.properties```
      
      Run this command on all the three instances.
      
  9.  Verify server(broker) work on this Kafka cluster:
  
      Open up a new terminal in one of the instance.
      
      ```bin/kafka-topics.sh --create --bootstrap-server 10.0.0.7:9092 --replication-factor 1 --partitions 1 --topic test-cluster```
      
      Change the --bootstrap-server ip to your own ip, any instance's ip will work.
  
       ```bin/kafka-console-producer.sh --bootstrap-server 10.0.0.7:9092 --topic test-cluster```
       
       Change the --bootstrap-server ip to your own ip, any instance's ip will work.
       
       Type any messages.
       
       Open up a new terminal on instance different from the one you just used to create topic and run producer.
       
       ```bin/kafka-console-consumer.sh --bootstrap-server 10.0.0.7:9092 --topic test --from-beginning```
      
       Change the --bootstrap-server ip to your own ip, any instance's ip will work.
       
       On the terminal, you will see the messages you just typed in.
       
