package bitfluc

import com.twosigma.flint.timeseries._
import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.SparkNLP
import org.apache.spark.sql._
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions.{col, udf}
import org.apache.hadoop.fs._
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
import dataload.DataLoader
import preprocess.Preprocessor
import dbconnector.DBConnector
import com.twosigma.flint.timeseries._
import scala.concurrent.duration._
import transform.Transform

object BitFluc
{
    def main(args: Array[String])
    {   
        val spark = getSparkSession()
        val dbconnect = new DBConnector(spark)
        val sentiment = loadNLPModel()
        
        val rcSchema = getRCSchema()
        val rcLoader = new DataLoader(spark, rcSchema)
        val rcPreprocessor = new Preprocessor(spark, rcLoader)
        val rcJsonPath = "s3a://gary-reddit-json/comments/*"
        val rcParquetPath = "s3a://gary-reddit-parquet/comment/part-*"

        val bpSchema = getBPSchema()
        val bpLoader = new DataLoader(spark, bpSchema)
        val bpPreprocessor = new Preprocessor(spark, bpLoader)
        val bpCsvPath = "s3a://gary-bitcoin-price-csv/bitcoin/bitcoin_price/*"
        val bpJsonPath = "s3a://gary-bitcoin-price-streaming/BP_2019-12/"

	rcloadPreprocess(rcPreprocessor, rcParquetPath, "parquet", sentiment)
	bploadPreprocess(bpPreprocessor, bpCsvPath, "csv")

	val timeList = List(("date", 3650, "ten_year", 1, 0.05)
                            ,("date", 1825, "five_year", 1, 0.05)
                            ,("date", 1095, "three_year", 1, 0.05)
                            ,("date", 365, "one_year", 1, 0.05)
                            ,("date", 180, "six_month", 1, 0.05)
                            ,("date", 90, "three_month", 1, 0.05)
                            ,("date,hour", 30, "one_month", 5, 0.008)
                            ,("date,hour", 5, "five_day", 5, 0.008))

        val reddit_comment = rcLoader.getData()
	val bitcoin_price = bpLoader.getData()
        reddit_comment.persist()
        bitcoin_price.persist()
  
	for((period, interval, dbtime, windowSize, threshold) <- timeList){
            val subredditList = List("all","bitcoin","cryptocurrency","ethereum","ripple")
            
            for(subreddit <- subredditList){ 
                val reddit_comment_subreddit = Transform.filterSubreddit(spark, reddit_comment, subreddit)
                val withSentiment = List("no_nlp", "with_nlp")
                
                for(isSentiment <- withSentiment){
                    val (reddit_comment_time, bitcoin_price_time) = Transform.timeJoin(spark, dbconnect,reddit_comment_subreddit, 
                                                                                        bitcoin_price, period, interval, dbtime, isSentiment, subreddit)
                    Transform.windowJoin(spark, dbconnect,reddit_comment_time, bitcoin_price_time, threshold, windowSize, period, dbtime, isSentiment, subreddit)
                }
            }
	}

        println("Finish Writing!!!!!!!!!!!!!!!!!!!!")
    } 
    
    // load pretrained nlp model. 
    def loadNLPModel(): PretrainedPipeline =
    {
        val sentiment = PretrainedPipeline
        .fromDisk("/usr/local/spark_nlp")

        sentiment
    }

    // preprocess unix time and create new column
    def datePreprocess(preprocessor: Preprocessor): Unit = 
    {
        preprocessor.convertUnixToPDT() 
        preprocessor.seperatePDT()
    }

    // find comments that only contain keywords
    def bodyPreprocess(preprocessor: Preprocessor): Unit =
    {
        preprocessor.filterKeyword()
        preprocessor.removeEmpty()
    }

    // add sentiment score
    def sentimentPreprocess(preprocessor: Preprocessor, sentiment: PretrainedPipeline): Unit =
    {
        preprocessor.addSentiment(sentiment)
        preprocessor.sentimentToNum()
    }

    // load reddit comment data and preprocess
    def rcloadPreprocess(preprocessor: Preprocessor, path: String, format: String, sentiment: PretrainedPipeline): Unit = 
    {
        load(preprocessor, path, format)
	rcPreprocess(preprocessor, sentiment)
    }

    // preprocess reddit comment data
    def rcPreprocess(preprocessor: Preprocessor, sentiment: PretrainedPipeline): Unit =
    {
       	datePreprocess(preprocessor)
        preprocessor.rcSelectColumn()
        bodyPreprocess(preprocessor)
        preprocessor.removeNegativeComment()
        preprocessor.removeDeletedAccount()
        sentimentPreprocess(preprocessor, sentiment)
    }

    // load bitcoin price data and preprocess
    def bploadPreprocess(preprocessor: Preprocessor, path: String, format: String): Unit =
    {
        load(preprocessor, path, format)
	bpPreprocess(preprocessor)
    }

    // preprocess bitcoin price data
    def bpPreprocess(preprocessor: Preprocessor): Unit =
    {
	preprocessor.removeInvalidData()
        datePreprocess(preprocessor)
        preprocessor.bpSelectColumn()
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
          .appName("all time interval, all subreddit, no sentiment, with persist")
          .master("spark://10.0.0.11:7077")
          .config("spark.default.parallelism", 400)  
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
	
        schema
    }
}

