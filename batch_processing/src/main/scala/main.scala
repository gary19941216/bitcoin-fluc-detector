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
import dataload.DataLoader
import preprocess.Preprocessor
import dbconnector.DBConnector
//import com.twosigma.flint.timeseries._
import scala.concurrent.duration._

object BitFluc
{
    def main(args: Array[String])
    {   
        val spark = getSparkSession()
        //val flintContext = FlintContext(spark)
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

        val period = "date"
        val interval = 1825
        val windowSize = 5
        val isPast = true


	rcloadPreprocess(rcPreprocessor, rcParquetPath, "parquet", period, interval, isPast, sentiment)
	bploadPreprocess(bpPreprocessor, bpCsvPath, "csv", period, interval, isPast, windowSize)

        val reddit_comment = rcLoader.getData()
        reddit_comment.explain()
        //reddit_comment.persist()
        reddit_comment.createOrReplaceTempView("reddit_comment")
  
        //val reddit_tsRDD = TimeSeriesRDD.fromDF(dataFrame = reddit_comment
        //                                       .select("score","spike"))(isSorted = true, timeUnit = MILLISECONDS)

        val bitcoin_price = bpLoader.getData()
        //bitcoin_price.persist()
        bitcoin_price.createOrReplaceTempView("bitcoin_price")
       
        //val bitcoin_tsRDD = TimeSeriesRDD.fromDF(dataFrame = bitcoin_price
        //                                        .select("price","spike"))(isSorted = true, timeUnit = MILLISECONDS)

        //val spikeDF = flintLeftJoin(reddit_tsRDD, bitcoin_tsRDD, "1min")

        val time_body_price = joinBitcoinReddit(spark)

        dbconnect.writeToCassandra(time_body_price.select("date","price","score"), "reddit_bitcoin_five_year_hour", "cycling")

        println("Finish Writing!!!!!!!!!!!!!!!!!!!!")

    } 

    // left join reddit_comment spike on bitcoin_price spike if reddit_commment date is same or before bitcoin_price date
    /*def flintLeftJoin(bitcoin_price: TimeSeriesRDD, reddit_comment: TimeSeriesRDD, tolerance: String): TimeSeriesRDD =
    {
        // e.g. tolerance='1day'
        //flint_df = flintContext.read.dataframe(df)
        val asOfJoinDF = bitcoin_price.leftJoin(reddit_comment, tolerance=tolerance)

        asOfJoinDF
    }*/

    // load pretrained nlp model. 
    def loadNLPModel(): PretrainedPipeline =
    {
        val sentiment = PretrainedPipeline
        .fromDisk("/usr/local/spark_nlp")

        sentiment
    }

    // join bitcoin and reddit dataset
    def joinBitcoinReddit(spark: SparkSession): DataFrame =
    {
       spark.sql("""
       SELECT BP.date, BP.hour, RC.score, BP.price
       FROM reddit_comment AS RC
       JOIN bitcoin_price AS BP ON RC.date=BP.date AND RC.hour=BP.hour
       """)
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

    // adding column retrieved from window function
    def windowPreprocess(preprocessor: Preprocessor, size: Int): Unit =
    {
        preprocessor.addWindowMax(size)
        preprocessor.addWindowMin(size)
        preprocessor.addSpike() 
    }

    // load reddit comment data and preprocess
    def rcloadPreprocess(preprocessor: Preprocessor, path: String, format: String, period: String, interval: Int, isPast: Boolean, sentiment: PretrainedPipeline): Unit = 
    {
        load(preprocessor, path, format)
	rcPreprocess(preprocessor, period, interval, isPast, sentiment)
    }

    // preprocess reddit comment data
    def rcPreprocess(preprocessor: Preprocessor, period: String, interval: Int, isPast: Boolean, sentiment: PretrainedPipeline): Unit =
    {
       	datePreprocess(preprocessor)
        preprocessor.selectColumn()
        bodyPreprocess(preprocessor)
        preprocessor.filterSubreddit()
        preprocessor.removeNegativeComment()
        preprocessor.removeDeletedAccount()
        sentimentPreprocess(preprocessor, sentiment)
        if(isPast){
            preprocessor.scoreInInterval(period ,interval)
            //preprocessor.nlpScoreInInterval(period ,interval) 
        }
    }

    // load bitcoin price data and preprocess
    def bploadPreprocess(preprocessor: Preprocessor, path: String, format: String, period: String, interval: Int, isPast: Boolean, size: Int): Unit =
    {
        load(preprocessor, path, format)
	bpPreprocess(preprocessor, period, interval, isPast, size)
    }

    // preprocess bitcoin price data
    def bpPreprocess(preprocessor: Preprocessor, period: String, interval: Int, isPast: Boolean, size: Int): Unit =
    {
	preprocessor.removeInvalidData()
        datePreprocess(preprocessor)
        if(isPast){
            preprocessor.priceInInterval(period, interval)
        }
        windowPreprocess(preprocessor, size)
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

