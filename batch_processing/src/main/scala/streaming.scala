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
                      .option("subscribe", "test")
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

        val rcLoader = new DataLoader(spark, rcSchema)
        val rcPreprocessor = new Preprocessor(spark, rcLoader)

        rcLoader.updateData(rcDF)
        BitFluc.rcPreprocess(rcPreprocessor, sentiment)

        val reddit_comment = rcLoader.getData()
        val reddit_comment_window = aggScore(reddit_comment)
        val reddit_comment_window_end = reddit_comment_window.select(col("window.end").alias("time"), col("score"))
	val reddit_comment_window_time = seperatePDT(reddit_comment_window_end).select("date","hour","minute","second","score")

	val query = reddit_comment_window_time.writeStream
        .foreachBatch { (batchDF, _) => 
            batchDF.printSchema()
            dbconnect.writeToCassandra(batchDF, "reddit_streaming_test3", "bitcoin_reddit")
        }.start()
        query.awaitTermination()
    }

    def seperatePDT(data: DataFrame): DataFrame =
    {
        val timeData = data.withColumn("date", to_date(col("time")))
                           .withColumn("hour", hour(col("time")))
                           .withColumn("minute", minute(col("time")))
                           .withColumn("second", second(col("time"))) 
	
	timeData
    }

    def aggScore(data: DataFrame): DataFrame = 
    {
        data
        .withColumn("timestamp", col("timestamp").cast(LongType).cast(TimestampType))
        .withWatermark("timestamp", "2 hours")
        .groupBy(
            window(col("timestamp"), "2 hours", "1 hour")
        )
        .agg(sum("score").alias("score")) 
    }

    def aggNLPScore(data: DataFrame): DataFrame = 
    {
 	data
        .withColumn("timestamp", col("timestamp").cast(LongType).cast(TimestampType))
        .withWatermark("timestamp", "2 hours")
        .groupBy(
            window(col("timestamp"), "2 hours", "1 hour")
        )
        .agg(sum(col("score")*col("sentiment_score")).alias("score")) 
    }
}
