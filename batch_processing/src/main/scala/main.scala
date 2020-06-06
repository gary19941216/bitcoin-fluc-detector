package bitfluc

import sys.process._
import java.net.URL
import java.io.File
import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types._
import org.apache.hadoop.fs._
import dataload.DataLoader

object BitFluc
{
    def main(args: Array[String])
    {   
        val spark = getSparkSession()
        val dataLoader = new DataLoader(spark)

        //val inputPath = "s3a://gary-bitcoin-price-csv/bitcoin/bitcoin_price/*"
        val inputPath = "s3a://gary-bitcoin-avro/*"
        //val inputPath = "s3a://gary-reddit-json/comments/RC_2014*.json"
        //val inputPath1 = "s3a://gary-reddit-parquet/comments/*.snappy.parquet"
        val outputPath = "s3a://gary-bitcoin-transaction-parquet/transaction"

        //dataLoader.loadURL(url)
        
        //dataLoader.showContent() 

        //inputrdd.foreach{ x => { println(x) } }
	//dataLoader.loadSchema(getBPSchema())
        dataLoader.loadAvro(inputPath).writeParquet(outputPath) 
        //dataLoader.loadAvro(inputPath)
        

        //dataLoader.loadParquet(inputPath1).show(3)
        //dataLoader.printSchema()
        //dataLoader.showCotent()
        //dataLoader.writeParquet(outputPath)                                
    }

    def getSparkSession(): SparkSession =  
    {
	val spark = SparkSession.builder
          .appName("Bit Fluc")
          .master("spark://10.0.0.11:7077")
          .config("spark.default.parallelism", 20)  
          .getOrCreate()	
	return spark 
    }

    // get bitcoin price schema
    def getBPSchema(): StructType = 
    {
        val schema = StructType(Seq(
    			StructField("utc", StringType, true),
    			StructField("price", FloatType, true),
    			StructField("volume", FloatType, true)))
	
	return schema
    }

    // get reddit comment schema
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

