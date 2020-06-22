package myapps;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.SdkClientException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ResponseHeaderOverrides;
import com.amazonaws.services.s3.model.S3Object;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import java.util.Properties;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;


public class redditProducer {

    public static void main(String[] args) throws IOException {
        
        // s3 bucket name 
	String bucketName = "gary-reddit-json";
	// s3 file key
        String key = "comments/RC_2019-11.json";
	// length of bytes
	long range = 100000000000000L;
	// Kafka topic
	String topic = "reddit";

	// s3 object initailizing
        S3Object fullObject = null, objectPortion = null, headerOverrideObject = null;
        try {
	    // initialize Amazon s3 client
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion("us-west-2")
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();

            // Get a range of bytes from an object.
            GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, key)
                    					.withRange(0, range);
	    // Get portion of Object
            objectPortion = s3Client.getObject(rangeObjectRequest);
	    // produce the content to Kafka topic
	    produce(objectPortion.getObjectContent(), topic);

        } catch (AmazonServiceException e) {
            // The call was transmitted successfully, but Amazon S3 couldn't process 
            // it, so it returned an error response.
            e.printStackTrace();
        } catch (SdkClientException e) {
            // Amazon S3 couldn't be contacted for a response, or the client
            // couldn't parse the response from Amazon S3.
            e.printStackTrace();
        } finally {
            // To ensure that the network connection doesn't remain open, close any open input streams.
            if (fullObject != null) {
                fullObject.close();
            }
            if (objectPortion != null) {
                objectPortion.close();
            }
            if (headerOverrideObject != null) {
                headerOverrideObject.close();
            }
        }
    }

    // produce s3 object content to Kafka topic
    private static void produce(InputStream input, String topicName) throws IOException {
	// create instance for properties to access producer configs   
	Properties props = new Properties();
	      
	// Assign ip for bootstrap servers
	props.put("bootstrap.servers", "10.0.0.7:9092");
	      
	// Set acknowledgements for producer requests.      
	props.put("acks", "all");
	      
	// If the request fails, the producer can automatically retry,
	props.put("retries", 1);
	      
	// Specify buffer size in config
	props.put("batch.size", 16382);
	      
	// Reduce the no of requests less than 0   
	props.put("linger.ms", 1);
	      
	// The buffer.memory controls the total amount of memory available to the producer for buffering.   
	props.put("buffer.memory", 33554432);
	      
	// serializer for key
	props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 
	// serializer for key
	props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	      
	// Creating a Kafka Producer with specified properties
	Producer<String, String> producer = new KafkaProducer<String, String>(props);
		   
        // Creating a InputStreamReader object	
	InputStreamReader isReader = new InputStreamReader(input);
        // Creating a BufferedReader object
        BufferedReader reader = new BufferedReader(isReader);
	// Creating a StringBuffer object
        StringBuffer sb = new StringBuffer();
	
	// Creating string for storing input string
	String str;
	// initialize key with 0
	int key = 0;
	// assign string from BufferedReader to str, and produce key-value pair to Kafka topic
        while((str = reader.readLine())!= null){
	    // producer send key-value pair to Kafka topic
            producer.send(new ProducerRecord<String, String>(topicName, Integer.toString(key++), str));
        }	
    }
}
