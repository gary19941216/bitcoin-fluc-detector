package streaming

import java.sql.Timestamp
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.hadoop.fs._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.streaming._
import dataload.DataLoader
import preprocess.Preprocessor
import dbconnector.DBConnector
import etl.ETL
import transform.Transform

// Spark Structured Streaming
object Streaming
{
    // get spark session
    private val spark = getSparkSession()

    // initialize DBConnector object
    private val dbconnect = new DBConnector(spark)

    // load spark session and dbconnect into Transform object
    private val transform = new Transform(spark, dbconnect)

    // load nlp model
    private val sentiment = ETL.loadNLPModel()

    // get schema of Reddit comment and Bitcoin price
    private val (rcSchema, bpSchema) = (ETL.getRCSchema(), ETL.getBPSchema())

    // intialize DataLoader for Reddit comment and Bitcoin Price
    private val (rcLoader, bpLoader) = (new DataLoader(spark, rcSchema), new DataLoader(spark, bpSchema))

    // intialize Preprocessor for Reddit comment and Bitcoin Price
    private val (rcPreprocessor, bpPreprocessor) = (new Preprocessor(spark, rcLoader), new Preprocessor(spark, bpLoader))

    // Kafka topic for Reddit
    private val redditTopic = "reddit"

    // Kafka topic for Bitcoin
    private val bitcoinTopic = "bitcoin"

    def main(args: Array[String])
    {

        // read streaming dataframe from Kafka topic
        val (rcStreamDF, bpStreamDF) = (readRedditStream(rcSchema, redditTopic), readBitcoinStream(bpSchema, bitcoinTopic))
        
        // load streaming dataframe into DataLoader
        rcLoader.updateData(rcStreamDF)
        bpLoader.updateData(bpStreamDF)

        // preprocess data in DataLoader
        ETL.rcPreprocess(rcPreprocessor, sentiment)
        ETL.bpPreprocess(bpPreprocessor)

        // get preprocessed data in DataLoader
        val (reddit_comment, bitcoin_price) = (rcLoader.getData(), bpLoader.getData())

        // get average price within the time window
        val bitcoin_price_window = avgPrice(bitcoin_price)

        // rename window.end column to time and select time and price column
	val bitcoin_price_window_end = bitcoin_price_window.select(col("window.end").alias("time"), col("price"))

        // seperate time column to date, hour, price and select them 
	val bitcoin_price_window_time = seperatePDT(bitcoin_price_window_end).select("date","hour","price")

        // start query for writing stream into Cassandra database
	val bitcoin_query = bitcoin_price_window_time.writeStream

        // microbatch processing for bitcoin
        .foreachBatch { (batchDF, _) => 
            val table = "bitcoin_streaming"
            val keyspace = "bitcoin_reddit"
            // append data to Cassandra
            dbconnect.writeToCassandra(batchDF, table, keyspace)
        }.start()

        // List for all of the Reddit query
        var redditQueryList  = List[StreamingQuery]()

        // target subreddit
        val subredditList = List("all","all_below","bitcoin","cryptocurrency","ethereum","ripple")

        // loop through all the subreddit
        for(subreddit <- subredditList){
        
            // get only relevant subreddit
            val reddit_comment_subreddit = transform.filterSubreddit(reddit_comment, subreddit)

            // List for using nlp sentiment analysis or not
            val withSentiment = List("no_nlp", "with_nlp")
            
            // loop through both of them
            for(isSentiment <- withSentiment){
		
                // get score sum within the time window
		val reddit_comment_window = sumScore(reddit_comment_subreddit)

                // rename window.end column to time and select time and score column
                val reddit_comment_window_end = reddit_comment_window.select(col("window.end").alias("time"), col("score"))

                // seperate time column to date, hour, price and select them
                val reddit_comment_window_time = seperatePDT(reddit_comment_window_end).select("date","hour","score")
	
                // start query for writing stream into Cassandra database
		val reddit_query = reddit_comment_window_time.writeStream

                // microbatch processing for reddit
		.foreachBatch { (batchDF, _) =>
	    	    val table = "reddit_streaming_" + subreddit + "_" + isSentiment
                    val keyspace = "bitcoin_reddit"
                    // append data to Cassandra
                    dbconnect.writeToCassandra(batchDF, table, keyspace)
		}.start()

                // add reddit query to list
		redditQueryList = reddit_query :: redditQueryList
            }
        }

        // await termination for bitcoin query
        bitcoin_query.awaitTermination()

        // await termination for all Reddit query
	for(reddit_query <- redditQueryList){
            reddit_query.awaitTermination()
        }
    }

    // read bitcoin stream data
    def readBitcoinStream(bpSchema: StructType, topic: String): DataFrame =
    {
        // get streaming dataframe from kafka topic
        val df = spark.readStream
                      .format("kafka")
                      .option("kafka.bootstrap.servers", "10.0.0.7:9092,10.0.0.10:9092,10.0.0.13:9092")
                      .option("subscribe", topic)
                      .option("startingOffsets","earliest")
                      .load()

        // cast key as string and read value with json format
        val bpKeyValue = df.select(
          col("key").cast("string"),
          from_json(col("value").cast("string"), bpSchema).alias("parsed_value")) 
        
        // select target column from previous dataframe
        val bpDF = bpKeyValue.select(
                   bpKeyValue.col("parsed_value.price"),
                   bpKeyValue.col("parsed_value.created_utc")
                   )
        
        bpDF 
    }

    // read reddit stream data
    def readRedditStream(rcSchema: StructType, topic: String): DataFrame =
    {
        // get streaming dataframe from kafka topic
        val df = spark.readStream
                      .format("kafka")
                      .option("kafka.bootstrap.servers", "10.0.0.7:9092,10.0.0.10:9092,10.0.0.13:9092")
                      .option("subscribe", topic)
                      .option("startingOffsets","earliest")
                      .load()
        // cast key as string and read value with json format
        val rcKeyValue = df.select(
          col("key").cast("string"),
          from_json(col("value").cast("string"), rcSchema).alias("parsed_value"))

        // select target column from previous dataframe
        val rcDF = rcKeyValue.select(
                   rcKeyValue.col("parsed_value.body"),
                   rcKeyValue.col("parsed_value.score"),
                   rcKeyValue.col("parsed_value.created_utc"),
                   rcKeyValue.col("parsed_value.subreddit"),
                   rcKeyValue.col("parsed_value.author")
                   )

	rcDF
    }

    // seperate date, hour, minute, second from time
    def seperatePDT(data: DataFrame): DataFrame =
    {
        val timeData = data.withColumn("date", to_date(col("time")))
                           .withColumn("hour", hour(col("time")))
                           .withColumn("minute", minute(col("time")))
                           .withColumn("second", second(col("time"))) 
	
	timeData
    }

    // aggreate price within window
    def avgPrice(data: DataFrame): DataFrame = 
    {
        data
        // create new column with Spark TimestampType
        .withColumn("bitcoin_timestamp", col("timestamp").cast(LongType).cast(TimestampType))
        // add on watermark to tolerance late data within 2 hours
        .withWatermark("bitcoin_timestamp", "2 hours")
        // group by window of 2 hours every 1 hour
        .groupBy(
            window(col("bitcoin_timestamp"), "2 hours", "1 hour")
        )
        // average the price and names it as price
        .agg(avg("price").alias("price"))
    }

    // aggregate reddit score within window
    def sumScore(data: DataFrame): DataFrame = 
    {
        data
        // create new column with Spark TimestampType
        .withColumn("reddit_timestamp", col("timestamp").cast(LongType).cast(TimestampType))
        // add on watermark to tolerance late data within 2 hours
        .withWatermark("reddit_timestamp", "2 hours")
        // group by window of 2 hours every 1 hour
        .groupBy(
            window(col("reddit_timestamp"), "2 hours", "1 hour")
        )
        // sum up the score and names it as score
        .agg(sum("score").alias("score")) 
    }

    // aggregate reddit nlp score within window
    def sumNLPScore(data: DataFrame): DataFrame = 
    {
 	data
        // create new column with Spark TimestampType
        .withColumn("reddit_timestamp", col("timestamp").cast(LongType).cast(TimestampType))
        // add on watermark to tolerance late data within 2 hours
        .withWatermark("reddit_timestamp", "2 hours")
        // group by window of 2 hours every 1 hour
        .groupBy(
            window(col("reddit_timestamp"), "2 hours", "1 hour")
        )
        // sum up the score with sentiment score and names it as score
        .agg(sum(col("score")*col("sentiment_score")).alias("score")) 
    }

    // get spark session for streaming
    def getSparkSession(): SparkSession =
    {
        val spark = SparkSession.builder
          .appName("spark streaming")
          // standalone mode with master ip and port
          .master("spark://10.0.0.11:7077")
          // set parallelism for spark job
          .config("spark.default.parallelism", 50)
          // set executor cores
          .config("spark.executor.cores", 4)
          // ip address for one of the cassandra seeds
          .config("spark.cassandra.connection.host", "10.0.0.6")
          .config("spark.cassandra.auth.username", "cassandra")
          .config("spark.cassandra.auth.password", "cassandra")
          .config("spark.cassandra.connection.port","9042")
          .config("spark.cassandra.output.consistency.level","ONE")
          .getOrCreate()

        // set log level to ERROR
        spark.sparkContext.setLogLevel("ERROR")

        spark
    }
}
