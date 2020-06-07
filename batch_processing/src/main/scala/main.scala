package bitfluc

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.hadoop.fs._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
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
        val rcJsonPath = "s3a://gary-reddit-json/comments/RC_2015*"
        val rcParquetPath = "s3a://gary-reddit-parquet/comments/part-0012*"
        datePreprocess(rcPreprocessor)

        val bpSchema = getBPSchema()
        val bpLoader = new DataLoader(spark, bpSchema)
        val bpPreprocessor = new Preprocessor(bpLoader)
        val bpCsvPath = "s3a://gary-bitcoin-price-csv/bitcoin/bitcoin_price/1coinUSD.csv/*"
        datePreprocess(bpPreprocessor)

        //bpPreprocessor.transformCsvToParquet(bpCsvPath, bpParquetPath)

        val reddit_comment = loadDFParquet(rcLoader, rcParquetPath)
        reddit_comment.createOrReplaceTempView("reddit_comment")
        val bitcoin_price = loadDFCsv(bpLoader, bpCsvPath)
        bitcoin_price.createOrReplaceTempView("bitcoin_price")

        

        //val rbJoinDF = rcTB.join(bpTP, rcTB("created_utc") === bpTP("utc"), "inner")
        /*val time_body_price = spark.sql("""
        SELECT BP.utc, RC.body, BP.price 
        FROM reddit_comment as RC
        FULL OUTER JOIN bitcoin_price as BP ON RC.created_utc=BP.created_utc
        """)

        time_body_price.explain()
        time_body_price.show(20)*/

    }

    // preprocess unix time and create new column
    def datePreprocess(preprocessor: Preprocessor): Unit = 
    {
        // create new column with PDT time
        preprocessor.convertUnixToPDT() 

        // create new column year, month, hour
        preprocessor.createYearMonthHour()
    }

    def loadPreprocess(): Unit = 
    {
        
    }

    // load DataFrame from a parquet file
    /*def loadDFParquet(loader: DataLoader, path: String): DataFrame = 
    {
       loader.loadParquet(path)
    }

    // load DataFrame from a json file   
    def loadDFJson(loader: DataLoader, path: String): DataFrame =
    {
       return loader.loadJson(path).getData()
    }

    // load DataFrame from a csv file
    def loadDFCsv(loader: DataLoader, path: String): DataFrame =
    {
       return loader.loadCsv(path).getData()  
    }

    def loadDFAvro(loader: DataLoader, path: String): DataFrame =
    {
       return loader.loadAvro(path).getData()                                                                  
    }*/

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
    			StructField("created_utc", StringType, true),
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

