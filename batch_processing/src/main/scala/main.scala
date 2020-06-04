package bitfluc

import sys.process._
import java.net.URL
import java.io.File
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import dataload.DataLoader

object BitFluc
{
    def main(args: Array[String])
    {   
        val spark = getSparkSession()
        val dataLoader = new DataLoader(spark, getRCSchema())

        //val inputPath = "s3a://gary-bitcoin-avro/bitcoin_data-000000000000.avro"
        val inputPath = "s3a://gary-reddit-json/comments/RC_2018-0*"
        val inputPath1 = "s3a://gary-reddit-parquet/comments/part-00000-0f16ed32-9a03-4965-81d9-ad67a1a1fe69-c000.snappy.parquet"
        //val outputPath = "s3a://gary-reddit-parquet/comments"

        //dataLoader.loadURL(url)
        
        //dataLoader.showContent()
        
	
        //dataLoader.loadJson(inputPath) 
        //dataLoader.loadAvro(inputPath)
        //dataLoader.printSchema()

        dataLoader.loadParquet(inputPath1)
        dataLoader.printSchema()
        /*dataLoader.showContent()
        dataLoader.writeParquet(outputPath)*/                                
    }

    def getSparkSession(): SparkSession =  
    {
	val spark = SparkSession.builder
          .appName("Bit Fluc")
          .getOrCreate()
	 
	return spark 
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
	return schema
    }
}

