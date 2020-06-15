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

import org.apache.spark.sql.streaming._

object BitFlucStreaming
{
    def main(args: Array[String])
    {
        val spark = BitFluc.getSparkSession()
        val dbconnect = new DBConnector(spark)
        val sentiment = BitFluc.loadNLPModel() 
       
        val df = spark.readStream
                      .format("kafka")
                      .option("kafka.bootstrap.servers", "10.0.0.7:9092,10.0.0.10:9092,10.0.0.13:9092")
                      .option("subscribe", "test4")
                      .option("startingOffsets","earliest")
                      .load()

        val rcSchema = BitFluc.getRCSchema()
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

        val period = "timestamp"
        val interval = -30
        val windowSize = 10
        val isPast = false

        val rcLoader = new DataLoader(spark, rcSchema)
        val rcPreprocessor = new Preprocessor(spark, rcLoader)

        rcLoader.updateData(rcDF)
        BitFluc.rcPreprocess(rcPreprocessor, isPast, sentiment)

        val reddit_comment = rcLoader.getData()
        val reddit_comment_window = aggScore(reddit_comment)
        val reddit_comment_window_end = reddit_comment_window.select(col("window.end").alias("date"), col("score"))
        reddit_comment_window.select(col("window.end").alias("date"), col("score")).printSchema()

        println("Finish Writing!!!!!!!!!!!!!!!!!!!!")

   	val query = reddit_comment_window_end.writeStream
	//.option("checkpointLocation", "/tmp/Spark/")
	.format("org.apache.spark.sql.cassandra")
	  .option("keyspace", "bitcoin_reddit")
	  .option("table", "reddit_streaming_test3")
	.outputMode("append")
	.start()

        query.awaitTermination()

       	/*val userSchema = new StructType().add("T_COUNTRY_NAME", "string").add("ORIGIN_COUNTRY_NAME", "string").add("count","integer")
	val csvDF = spark
  	.readStream
  	.option("sep", ";")
  	.schema(userSchema)      // Specify schema of the csv files
  	.csv("s3a://gary-bitcoin-price-csv/bitcoin/bitcoin_price/coinbaseUSD.csv") 
        //.csv("/usr/local/spark/summary.csv")

        csvDF.printSchema()*/

	/*val query = reddit_comment_window.writeStream.format("console")
	.trigger(Trigger.ProcessingTime(10000))
	.outputMode(OutputMode.Append)
	.start()*/

	/*val query = reddit_comment_window_end.writeStream
  	.outputMode("complete")
  	.format("console")
  	.start()

	query.awaitTermination()*/


	/*reddit_comment.writeStream
        .foreachBatch { (batchDF, _) => 
            batchDF.printSchema()
            dbconnect.writeToCassandra(batchDF.select("date","hour","minute","second","score")
                                       //.withColumn("timestamp", col("timestamp").cast(LongType).cast(TimestampType))
                                       //.withWatermark("timestamp", "1 minutes").printSchema()
                                       , "reddit_streaming_test2", "cycling")
        }.start()*/
    }

    def aggScore(data: DataFrame): DataFrame = 
    {
        data
        .withColumn("timestamp", col("timestamp").cast(LongType).cast(TimestampType))
        .withWatermark("timestamp", "2 minutes")
        .groupBy(
            window(col("timestamp"), "1 minutes", "1 minutes")
        )
        .agg(sum("score").alias("score")) 
    }

    def aggNLPScore(data: DataFrame): DataFrame = 
    {
 	data
        .withColumn("timestamp", col("timestamp").cast(LongType).cast(TimestampType))
        .withWatermark("timestamp", "2 minutes")
        .groupBy(
            window(col("timestamp"), "1 minutes", "1 minutes")
        )
        .agg(sum(col("score")*col("sentiment_score")).alias("score")) 
    }
}
