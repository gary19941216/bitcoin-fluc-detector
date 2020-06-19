package bitflucstreaming

import java.sql.Timestamp
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.hadoop.fs._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import dataload.DataLoader
import preprocess.Preprocessor
import dbconnector.DBConnector
import bitfluc.BitFluc
import transform.Transform

import org.apache.spark.sql.streaming._

object BitFlucStreaming
{
    def main(args: Array[String])
    {
        val spark = getSparkSession()
        val dbconnect = new DBConnector(spark)
        val sentiment = BitFluc.loadNLPModel() 
      
        val rcSchema = BitFluc.getRCSchema()
        val bpSchema = BitFluc.getBPSchema()

        val rcLoader = new DataLoader(spark, rcSchema)
        val rcPreprocessor = new Preprocessor(spark, rcLoader)
        val bpLoader = new DataLoader(spark, bpSchema)
        val bpPreprocessor = new Preprocessor(spark, bpLoader)

        val rcDF = readRedditStream(spark, rcSchema, "test11")
        val bpDF = readBitcoinStream(spark, bpSchema, "test18")
        
        rcLoader.updateData(rcDF)
        bpLoader.updateData(bpDF)
        
        BitFluc.rcPreprocess(rcPreprocessor, sentiment)
        BitFluc.bpPreprocess(bpPreprocessor)

        val reddit_comment = rcLoader.getData()
        val bitcoin_price = bpLoader.getData()

        val bitcoin_price_window = avgPrice(bitcoin_price)
	val bitcoin_price_window_end = bitcoin_price_window.select(col("window.end").alias("time"), col("price"))
	val bitcoin_price_window_time = seperatePDT(bitcoin_price_window_end).select("date","hour","price")

	val bitcoin_query = bitcoin_price_window_time.writeStream
        .foreachBatch { (batchDF, _) => 
            batchDF.printSchema()
            dbconnect.writeToCassandra(batchDF, "bitcoin_streaming_test", "bitcoin_reddit")
        }.start()

        var redditQueryList  = List[StreamingQuery]()

        val subredditList = List("all","bitcoin","cryptocurrency","ethereum","ripple")

        for(subreddit <- subredditList){
            val reddit_comment_subreddit = Transform.filterSubreddit(spark, reddit_comment, subreddit)
            val withSentiment = List("no_nlp", "with_nlp")

            for(isSentiment <- withSentiment){
		
		val reddit_comment_window = sumScore(reddit_comment_subreddit)
                val reddit_comment_window_end = reddit_comment_window.select(col("window.end").alias("time"), col("score"))
                val reddit_comment_window_time = seperatePDT(reddit_comment_window_end).select("date","hour","score")
	
		val reddit_query = reddit_comment_window_time.writeStream
		.foreachBatch { (batchDF, _) =>
		    batchDF.printSchema()
	    	    dbconnect.writeToCassandra(batchDF, "reddit_streaming_test_" + subreddit + "_" + isSentiment, "bitcoin_reddit")
		}start()

		redditQueryList = reddit_query :: redditQueryList
            }
        }

        bitcoin_query.awaitTermination()

	for(reddit_query <- redditQueryList){
            reddit_query.awaitTermination()
        }
    }

    // read bitcoin stream data
    def readBitcoinStream(spark: SparkSession, bpSchema: StructType, topic: String): DataFrame =
    {
        val df = spark.readStream
                      .format("kafka")
                      .option("kafka.bootstrap.servers", "10.0.0.7:9092,10.0.0.10:9092,10.0.0.13:9092")
                      .option("subscribe", topic)
                      .option("startingOffsets","earliest")
                      .load()
        
        val bpKeyValue = df.select(
          col("key").cast("string"),
          from_json(col("value").cast("string"), bpSchema).alias("parsed_value")) 
        
        val bpDF = bpKeyValue.select(
                   bpKeyValue.col("parsed_value.price"),
                   bpKeyValue.col("parsed_value.created_utc")
                   )
        
        bpDF 
    }

    // read reddit stream data
    def readRedditStream(spark: SparkSession, rcSchema: StructType, topic: String): DataFrame =
    {
        val df = spark.readStream
                      .format("kafka")
                      .option("kafka.bootstrap.servers", "10.0.0.7:9092,10.0.0.10:9092,10.0.0.13:9092")
                      .option("subscribe", topic)
                      .option("startingOffsets","earliest")
                      .load()

        val rcKeyValue = df.select(
          col("key").cast("string"),
          from_json(col("value").cast("string"), rcSchema).alias("parsed_value"))

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
        .withColumn("bitcoin_timestamp", col("timestamp").cast(LongType).cast(TimestampType))
        .withWatermark("bitcoin_timestamp", "2 hours")
        .groupBy(
            window(col("bitcoin_timestamp"), "2 hours", "1 hour")
        )
        .agg(avg("price").alias("price"))
    }

    // aggregate reddit score within window
    def sumScore(data: DataFrame): DataFrame = 
    {
        data
        .withColumn("reddit_timestamp", col("timestamp").cast(LongType).cast(TimestampType))
        .withWatermark("reddit_timestamp", "2 hours")
        .groupBy(
            window(col("reddit_timestamp"), "2 hours", "1 hour")
        )
        .agg(sum("score").alias("score")) 
    }

    // aggregate reddit nlp score within window
    def sumNLPScore(data: DataFrame): DataFrame = 
    {
 	data
        .withColumn("reddit_timestamp", col("timestamp").cast(LongType).cast(TimestampType))
        .withWatermark("reddit_timestamp", "2 hours")
        .groupBy(
            window(col("reddit_timestamp"), "2 hours", "1 hour")
        )
        .agg(sum(col("score")*col("sentiment_score")).alias("score")) 
    }

    def getSparkSession(): SparkSession =
    {
        val spark = SparkSession.builder
          .appName("spark streaming")
          .master("spark://10.0.0.11:7077")
          //.config("spark.executor.cores", 18)
          .config("spark.default.parallelism", 400)
          .config("spark.cassandra.connection.host", "10.0.0.6")
          .config("spark.cassandra.auth.username", "cassandra")
          .config("spark.cassandra.auth.password", "cassandra")
          .config("spark.cassandra.connection.port","9042")
          .config("spark.cassandra.output.consistency.level","ONE")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        spark
    }
}
