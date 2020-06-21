package etl

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
import scala.concurrent.duration._
import transform.Transform

object ETL
{
    // get spark session
    private val spark = getSparkSession()

    // initialize DBConnector object
    private val dbconnect = new DBConnector(spark)

    // load spark session and dbconnect into Transform object
    private val transform = new Transform(spark, dbconnect)

    // load nlp model
    private val sentiment = loadNLPModel()

    // get schema of Reddit comment and Bitcoin price
    private val (rcSchema, bpSchema) = (ETL.getRCSchema(), ETL.getBPSchema())

    // intialize DataLoader for Reddit comment and Bitcoin Price
    private val (rcLoader, bpLoader) = (new DataLoader(spark, rcSchema), new DataLoader(spark, bpSchema))

    // intialize Preprocessor for Reddit comment and Bitcoin Price
    private val (rcPreprocessor, bpPreprocessor) = (new Preprocessor(spark, rcLoader), new Preprocessor(spark, bpLoader))

    // path to reddit comment parquet data and path to bitcoin price csv data
    private val (rcParquetPath, bpCsvPath) = ("s3a://gary-reddit-parquet/comment/part-*", "s3a://gary-bitcoin-price-csv/bitcoin/bitcoin_price/*")
 
    def main(args: Array[String])
    {

	// load and preprocess reddit comment data
	rcloadPreprocess(rcPreprocessor, rcParquetPath, "parquet", sentiment)

	// load and preprocess bitcoin price data
	bploadPreprocess(bpPreprocessor, bpCsvPath, "csv")

	// List of different time interval and period
	val timeList = List( //("date", 3650, "ten_year", 1, 0.05)
                            ("date", 1825, "five_year", 1, 0.05)
                            ,("date", 1095, "three_year", 1, 0.05)
                            ,("date", 365, "one_year", 1, 0.05)
                            ,("date", 180, "six_month", 1, 0.05)
                            ,("date", 90, "three_month", 1, 0.05)
                            ,("date,hour", 30, "one_month", 5, 0.02)
                            ,("date,hour", 5, "five_day", 5, 0.02))

        // get reddit comment data from dataloader
        val reddit_comment = rcLoader.getData()

        // get bitcoin price data from dataloader
	val bitcoin_price = bpLoader.getData()

        // persist preprocessed data
        reddit_comment.persist()
        bitcoin_price.persist()
  
        // loop through the list
	for((period, interval, dbtime, windowSize, threshold) <- timeList){
            
            // target subreddit
            val subredditList = List("all","all_below","bitcoin","cryptocurrency","ethereum","ripple")
            
            // loop through subreddit list
            for(subreddit <- subredditList){

                // get only relevant subreddit
                val reddit_comment_subreddit = transform.filterSubreddit(reddit_comment, subreddit)

                // List for using nlp sentiment analysis or not
                val withSentiment = List("no_nlp", "with_nlp")
                
                // loop through both of them
                for(isSentiment <- withSentiment){

                    // join reddit and bitcoin data and write them into cassandra
                    val (reddit_comment_time, bitcoin_price_time) = transform.timeJoin(reddit_comment_subreddit, bitcoin_price, 
                                                                                       period, interval, dbtime, isSentiment, subreddit)

                    // calculate and update the spike count to cassandra
                    transform.windowJoin(reddit_comment_time, bitcoin_price_time, threshold, windowSize, period, dbtime, isSentiment, subreddit)
                }
            }
	}

    } 
    
    // load pretrained nlp model. 
    def loadNLPModel(): PretrainedPipeline =
    {
        val sentiment = PretrainedPipeline
        // path to downloaded nlp data
        .fromDisk("/usr/local/spark_nlp")

        sentiment
    }

    // preprocess unix time and create new column
    def datePreprocess(preprocessor: Preprocessor): Unit = 
    {
        // convert unix time to PDT time
        preprocessor.convertUnixToPDT() 

        // seperate PDT time into date, hour, minute, second column
        preprocessor.seperatePDT()
    }

    // find comments that only contain keywords
    def bodyPreprocess(preprocessor: Preprocessor): Unit =
    {
        // get comment with body contaning keywords
        preprocessor.filterKeyword()

        // remove comment with empty body
        preprocessor.removeEmpty()
    }

    // add sentiment score
    def sentimentPreprocess(preprocessor: Preprocessor, sentiment: PretrainedPipeline): Unit =
    {
        // add sentiment column
        preprocessor.addSentiment(sentiment)

        // transform sentiment string to number
        preprocessor.sentimentToNum()
    }

    // load reddit comment data and preprocess
    def rcloadPreprocess(preprocessor: Preprocessor, path: String, format: String, sentiment: PretrainedPipeline): Unit = 
    {
        // load data according to format
        load(preprocessor, path, format)

        // preprocess reddit comment data
	rcPreprocess(preprocessor, sentiment)
    }

    // preprocess reddit comment data
    def rcPreprocess(preprocessor: Preprocessor, sentiment: PretrainedPipeline): Unit =
    {
        // preprocess date
       	datePreprocess(preprocessor)

        // select target column
        preprocessor.rcSelectColumn()

        // preprocess body column
        bodyPreprocess(preprocessor)

        // remove comment with negative score
        preprocessor.removeNegativeComment()

        // remove comment from deleted account
        preprocessor.removeDeletedAccount()

        // preprocess sentiment
        sentimentPreprocess(preprocessor, sentiment)
    }

    // load bitcoin price data and preprocess
    def bploadPreprocess(preprocessor: Preprocessor, path: String, format: String): Unit =
    {
        // load data according to format
        load(preprocessor, path, format)

        // preprocess bitcoin price data
	bpPreprocess(preprocessor)
    }

    // preprocess bitcoin price data
    def bpPreprocess(preprocessor: Preprocessor): Unit =
    {
        // remove irrelavant data
	preprocessor.removeInvalidData()

        // preprocess date
        datePreprocess(preprocessor)

        // select target column
        preprocessor.bpSelectColumn()
    }

    // load data for different format
    def load(preprocessor: Preprocessor, path: String, format: String): Unit =
    {
        // load json format data
        if(format == "json"){
            preprocessor.dataloader.loadJson(path)
        // load parquet format data
        } else if(format == "parquet"){
            preprocessor.dataloader.loadParquet(path)
        // load csv format data
        } else if(format == "csv"){
            preprocessor.dataloader.loadCsv(path)
        // load avro format data
        } else {
            preprocessor.dataloader.loadAvro(path)
        } 
    }

    // get SparkSession with configuration
    def getSparkSession(): SparkSession =  
    {
	val spark = SparkSession.builder
          .appName("historical data ETL")
          // standalone mode with master ip and port
          .master("spark://10.0.0.11:7077")
          // set parallelism for spark job
          .config("spark.default.parallelism", 400)  
          // ip address for one of the cassandra seeds
          .config("spark.cassandra.connection.host", "10.0.0.6")
          .config("spark.cassandra.auth.username", "cassandra")            
          .config("spark.cassandra.auth.password", "cassandra") 
	  .config("spark.cassandra.connection.port","9042")
          .config("spark.cassandra.output.consistency.level","ONE")
          .getOrCreate()

        // set log level to ERROR
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

