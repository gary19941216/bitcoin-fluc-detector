package bitfluc

import sys.process._
import java.net.URL
import java.io.File
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.hadoop.fs._
import dataload.DataLoader
import preprocess.Preprocessor

object BitFluc
{
    def main(args: Array[String])
    {   
        val spark = getSparkSession()
        
        val rcSchema = getRCSchema()
        val rcLoader = new DataLoader(spark, rcSchema)
        val rcPreprocessor = new Preprocessor(rcLoader)

        val bpSchema = getBPSchema()
        val bpLoader = new DataLoader(spark, bpSchema)
        val bpPreprocessor = new Preprocessor(bpLoader)
        //val inputPath = "s3a://gary-bitcoin-avro/*"
        //val inputPath1 = "s3a://gary-reddit-parquet/comments/*.snappy.parquet"
        val rcJsonPath = "s3a://gary-reddit-json/comments/RC_2015*"
        val rcParquetPath = "s3a://gary-reddit-parquet/comments/part-00000*"
        val bpCsvPath = "s3a://gary-bitcoin-price-csv/bitcoin/bitcoin_price/1coinUSD.csv/*"
        val bpParquetPath = "s3a://gary-bitcoin-price-parquet/bitcoin/*"

        //bpPreprocessor.transformCsvToParquet(bpCsvPath, bpParquetPath)

        //val rcDF = loadDFJson(rcLoader, rcJsonPath)
        //val rc_time_body = rcDF.select("created_utc", "body")
        val bpDF = loadDFCsv(bpLoader, bpParquetPath)
        val bp_time_price = bpDF.select("utc","price")

        bpDF.show(10)

        /*val rbJoinDF = rcDF.join(bpDF, rcDF("created_utc") === bpDF("utc"), "outer")

        rbJoinDF.explain()
        rbJoinDF.show(5)*/

    }

    // load DataFrame from a parquet file
    def loadDFParquet(loader : DataLoader, path : String): Dataset[Row] = 
    {
       return loader.loadParquet(path).getData()
    }

    // load DataFrame from a json file   
    def loadDFJson(loader : DataLoader, path : String): Dataset[Row] =
    {
       return loader.loadJson(path).getData()
    }

    // load DataFrame from a csv file
    def loadDFCsv(loader : DataLoader, path : String): Dataset[Row] =
    {
       return loader.loadCsv(path).getData()  
    }

    def loadDFAvro(loader : DataLoader, path : String): Dataset[Row] =
    {
       return loader.loadAvro(path).getData()                                                                  
    }

    // get SparkSession with configuration
    def getSparkSession(): SparkSession =  
    {
	val spark = SparkSession.builder
          .appName("Bit Fluc")
          //.master("spark://10.0.0.11:7077")
          .config("spark.default.parallelism", 20)  
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")
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

