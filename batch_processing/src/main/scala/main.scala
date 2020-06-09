package bitfluc

import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.hadoop.fs._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import dataload.DataLoader
import preprocess.Preprocessor
import dbconnector.DBConnector

object BitFluc
{
    def main(args: Array[String])
    {   
        val spark = getSparkSession()
        val dbconnect = new DBConnector(spark)
        
        val rcSchema = getRCSchema()
        val rcLoader = new DataLoader(spark, rcSchema)
        val rcPreprocessor = new Preprocessor(rcLoader)
        val rcJsonPath = "s3a://gary-reddit-json/comments/*"
        val rcParquetPath = "s3a://gary-reddit-parquet/comments/part-00115"

        val bpSchema = getBPSchema()
        val bpLoader = new DataLoader(spark, bpSchema)
        val bpPreprocessor = new Preprocessor(bpLoader)
        val bpCsvPath = "s3a://gary-bitcoin-price-csv/bitcoin/bitcoin_price/*"

	rcloadPreprocess(rcPreprocessor, rcJsonPath, "json")
	bploadPreprocess(bpPreprocessor, bpCsvPath, "csv")

        val reddit_comment = rcLoader.getData()
        reddit_comment.show(5)
        reddit_comment.createOrReplaceTempView("reddit_comment")

        //writeToCassandra(reddit_comment)*/

        val bitcoin_price = bpLoader.getData()
        bitcoin_price.show(5)
        bitcoin_price.createOrReplaceTempView("bitcoin_price")
        
        /*dbconnect.writeToCassandra(bitcoin_price.select("date","price","volume"), "bitcoin", "cycling")
        val bpDF = dbconnect.readFromCassandra("bitcoin", "cycling")
        print(bpDF.count())*/
        
        val time_body_price = spark.sql("""
        SELECT BP.date, BP.hour, RC.body, BP.price 
        FROM reddit_comment AS RC
        JOIN bitcoin_price AS BP ON RC.date=BP.date
        """)

        time_body_price.explain()
        time_body_price.show(20)

    }

    // preprocess unix time and create new column
    def datePreprocess(preprocessor: Preprocessor): Unit = 
    {
        preprocessor.convertUnixToPDT() 
        preprocessor.seperatePDT()
    }

    // find comments in only relevant subreddit
    def subredditPreprocess(preprocessor: Preprocessor): Unit = 
    {
        preprocessor.filterSubreddit()
    }

    // find comments that only contain keywords
    def bodyPreprocess(preprocessor: Preprocessor): Unit =
    {
        preprocessor.filterKeyword()
        preprocessor.removeEmpty()
    }

    // load reddit comment data and preprocess
    def rcloadPreprocess(preprocessor: Preprocessor, path: String, format: String): Unit = 
    {
        load(preprocessor, path, format)
        datePreprocess(preprocessor)
        subredditPreprocess(preprocessor)
        bodyPreprocess(preprocessor)
    }

    // load bitcoin price data and preprocess
    def bploadPreprocess(preprocessor: Preprocessor, path: String, format: String): Unit =
    {
        load(preprocessor, path, format)
        datePreprocess(preprocessor)
    }

    // load data for different format
    def load(preprocessor: Preprocessor, path: String, format: String): Unit =
    {
        if(format == "json"){
          preprocessor.dataloader.loadJson(path)
        } else if(format == "parquet"){
          preprocessor.dataloader.loadParquet(path)
        } else if(format == "csv"){
          preprocessor.dataloader.loadCsv(path)
        } else {
          preprocessor.dataloader.loadAvro(path)
        } 
    }

    // get SparkSession with configuration
    def getSparkSession(): SparkSession =  
    {
	val spark = SparkSession.builder
          .appName("Bit Fluc")
          //.master("spark://10.0.0.11:7077")
          .config("spark.default.parallelism", 20)  
          .config("spark.cassandra.connection.host", "10.0.0.5")
          .config("spark.cassandra.auth.username", "cassandra")            
          .config("spark.cassandra.auth.password", "cassandra") 
	  .config("spark.cassandra.connection.port","9042")
          .config("spark.cassandra.output.consistency.level","ONE")
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

