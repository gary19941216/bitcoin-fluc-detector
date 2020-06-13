package bitflucstreaming

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.hadoop.fs._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import dataload.DataLoader
import preprocess.Preprocessor
import dbconnector.DBConnector

object BitFlucStreaming
{
    def main(args: Array[String])
    {
        val spark = getSparkSession()
        import spark.implicits._
        //val flintContext = FlintContext(spark)
        val dbconnect = new DBConnector(spark)
        //val sentiment = loadNLPModel()
        
        val df = spark.readStream
                      .format("kafka")
                      .option("kafka.bootstrap.servers", "10.0.0.7:9092,10.0.0.10:9092,10.0.0.13:9092")
                      .option("subscribe", "test")
                      .load()

        df.selectExpr("CAST(value AS STRING)").as[(String)]
		
	df.writeStream
    	.format("console")
    	.option("truncate","false")
    	.start()
    	.awaitTermination()

    }

    def getSparkSession(): SparkSession =
    {
        val spark = SparkSession.builder
          .appName("Bit Fluc")
          .master("spark://10.0.0.11:7077")
          .config("spark.default.parallelism", 50)
          .config("spark.cassandra.connection.host", "10.0.0.5")
          .config("spark.cassandra.auth.username", "cassandra")
          .config("spark.cassandra.auth.password", "cassandra")
          .config("spark.cassandra.connection.port","9042")
          .config("spark.cassandra.output.consistency.level","ONE")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        spark
    }

    // get bitcoin price schema
    def getBPSchema(): StructType =
    {
        val schema = StructType(Seq(
                        StructField("created_utc", StringType, true),
                        StructField("price", FloatType, true),
                        StructField("volume", FloatType, true)))

        schema
    }

    def getRCSchema(): StructType =
    {
        val schema = StructType(Seq(
                        StructField("archived", BooleanType, true),
                        StructField("author", StringType, true),
                        StructField("author_flair_css_class", StringType, true),
                        StructField("author_flair_text", StringType, true),
                        StructField("body", StringType, true),
                        StructField("controversiality", LongType, true),
                        StructField("created_utc", StringType, true),
                        StructField("distinguished", StringType, true),
                        StructField("downs", LongType, true),
                        StructField("edited", StringType, true),
                        StructField("gilded", LongType, true),
                        StructField("id", StringType, true),
                        StructField("link_id", StringType, true),
                        StructField("name", StringType, true),
                        StructField("parent_id", StringType, true),
                        StructField("permalink", StringType, true),
                        StructField("retrieved_on", LongType, true),
                        StructField("score", LongType, true),
                        StructField("score_hidden", BooleanType, true),
                        StructField("stickied", BooleanType, true),
                        StructField("subreddit", StringType, true),
                        StructField("subreddit_id", StringType, true),
                        StructField("ups", LongType, true)))
        schema
    }
}
