## **How to read file from s3 and produce to Kafka topic**

  1.  Configure s3 setting and Kafka topic:
  
      In ```producer/src/main/java/apps/redditProducer.java```
      
      ```bash
      // s3 bucket name 
      String bucketName = "gary-reddit-json";
      // s3 file key
      String key = "comments/RC_2019-11.json";
      // length of bytes
      long range = 100000000000000L;
      // Kafka topic
      String topic = "reddittest";
      ```
      Change the configuration to your own.
      
      ```range``` will determine how many bytes you are going to produce.
      
      ```bash
      // Assign ip for bootstrap servers
	    props.put("bootstrap.servers", "10.0.0.7:9092");
      ```
      
      Change the bootstrap servers ip to your own.
      
  2.  Compile Java code with maven:
  
      run the command under ```producer``` folder.
  
      ```mvn clean package```
      
  3.  Run the Java code with maven:
  
      run the command under ```producer``` folder.
      
      ```mvn exec:java -Dexec.mainClass=myapps.redditProducer```
      
      Change ```redditProducer``` to your own java file.
      
  4.  Verify it's working:
  
      ```bin/kafka-console-producer.sh --bootstrap-server 10.0.0.7:9092 --topic producer-test```
      
      Change ```--bootstrap-server``` ip to your own ip, and change ```producer-test``` to your Kafka topic.
