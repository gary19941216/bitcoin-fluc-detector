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

//import util.properties.packages;
import java.util.Properties;

//import simple producer packages
import org.apache.kafka.clients.producer.Producer;

//import KafkaProducer packages
import org.apache.kafka.clients.producer.KafkaProducer;

//import ProducerRecord packages
import org.apache.kafka.clients.producer.ProducerRecord;


public class bitcoinProducer {

    public static void main(String[] args) throws IOException {
        Regions clientRegion = Regions.DEFAULT_REGION;
        String bucketName = "gary-reddit-json";
        String key = "comments/RC_2019-11.json";
	long range = 100000000000000L;
	//Thread.currentThread().setContextClassLoader(null);

        S3Object fullObject = null, objectPortion = null, headerOverrideObject = null;
        try {
            AmazonS3 s3Client = AmazonS3ClientBuilder.standard()
                    .withRegion("us-west-2")
                    .withCredentials(new ProfileCredentialsProvider())
                    .build();

            // Get a range of bytes from an object and print the bytes.
            GetObjectRequest rangeObjectRequest = new GetObjectRequest(bucketName, key)
                    .withRange(0, range);
            objectPortion = s3Client.getObject(rangeObjectRequest);
	    produce(objectPortion.getObjectContent());

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

    private static void produce(InputStream input) throws IOException {
	//Assign topicName to string variable
	String topicName = "test";

	// create instance for properties to access producer configs   
	Properties props = new Properties();
	      
	//Assign localhost id
	props.put("bootstrap.servers", "10.0.0.7:9092");
	      
	//Set acknowledgements for producer requests.      
	props.put("acks", "all");
	      
	//If the request fails, the producer can automatically retry,
	props.put("retries", 1);
	      
	//Specify buffer size in config
	props.put("batch.size", 16384);
	      
	//Reduce the no of requests less than 0   
	props.put("linger.ms", 1);
	      
	//The buffer.memory controls the total amount of memory available to the producer for buffering.   
	props.put("buffer.memory", 33554432);
	      
	props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
		 
	props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");
	      
	Producer<String, String> producer = new KafkaProducer<String, String>(props);
		    
	InputStreamReader isReader = new InputStreamReader(input);
        //Creating a BufferedReader object
        BufferedReader reader = new BufferedReader(isReader);
        StringBuffer sb = new StringBuffer();
        String str;
	int i = 0;
        while((str = reader.readLine())!= null){
            producer.send(new ProducerRecord<String, String>(topicName,
	    Integer.toString(i++), str));
        }	

    }

}
