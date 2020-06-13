package bitfluc

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
        val rcJsonPath = "s3a://gary-reddit-json/comments/RC_2015-01.json"
        val rcParquetPath = "s3a://gary-reddit-parquet/comment/part-*"

        val bpSchema = getBPSchema()
        val bpLoader = new DataLoader(spark, bpSchema)
        val bpPreprocessor = new Preprocessor(spark, bpLoader)
        val bpCsvPath = "s3a://gary-bitcoin-price-csv/bitcoin/bitcoin_price/*"

        val period = "date"
        val interval = 1095
        val windowSize = 10

	rcloadPreprocess(rcPreprocessor, rcParquetPath, "parquet", period, interval)
	bploadPreprocess(bpPreprocessor, bpCsvPath, "csv", period, interval, windowSize)

        val reddit_comment = rcLoader.getData()
        reddit_comment.explain()
        //reddit_comment.persist()
        reddit_comment.createOrReplaceTempView("reddit_comment")

        val bitcoin_price = bpLoader.getData()
        //bitcoin_price.persist()
        bitcoin_price.createOrReplaceTempView("bitcoin_price")
  
        /*val bpDF = dbconnect.readFromCassandra("bitcoin_price_float_three_years_test", "cycling")
                            
        print(bpDF.count())*/
        
        val time_body_price = joinBitcoinReddit(spark)

        dbconnect.writeToCassandra(time_body_price.select("date","price","score"), "reddit_bitcoin_three_years_test", "cycling")

        println("Finish Writing!!!!!!!!!!!!!!!!!!!!")

        /*time_body_price.explain()
        time_body_price.show(20)*/

    } 

    // left join reddit_comment spike on bitcoin_price spike if reddit_commment date is same or before bitcoin_price date
    /*def flintLeftJoin(bitcoin_price: DataFrame, reddit_comment: DataFrame, tolerance: String): DataFrame =
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
        .fromDisk("../scala/Insight_Project/bitcoin-fluc-detector/batch_processing/target/scala-2.11/spark_nlp/sentiment_analysis")

        sentiment
    }

    // join bitcoin and reddit dataset
    def joinBitcoinReddit(spark: SparkSession): DataFrame =
    {
       spark.sql("""
       SELECT BP.date, RC.score, BP.price
       FROM reddit_comment AS RC
       JOIN bitcoin_price AS BP ON RC.date=BP.date
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

    // adding column retrieved from window function
    def windowPreprocess(preprocessor: Preprocessor, size: Int, period: Int): Unit =
    {
        preprocessor.addWindowMax(size)
        preprocessor.addWindowMin(size)
        //preprocessor.addSpike(period) 
    }

    // load reddit comment data and preprocess
    def rcloadPreprocess(preprocessor: Preprocessor, path: String, format: String, period: String, interval: Int): Unit = 
    {
        load(preprocessor, path, format)
        datePreprocess(preprocessor)
        preprocessor.selectColumn()
        //bodyPreprocess(preprocessor)
        preprocessor.filterSubreddit()
        preprocessor.removeNegativeComment()
        preprocessor.removeDeletedAccount()
        //bodyPreprocess(preprocessor)
        preprocessor.scoreInInterval(period,interval)
    }

    // load bitcoin price data and preprocess
    def bploadPreprocess(preprocessor: Preprocessor, path: String, format: String, period: String, interval: Int, size: Int): Unit =
    {
        load(preprocessor, path, format)
        preprocessor.removeInvalidData()
        datePreprocess(preprocessor)
        preprocessor.priceInInterval(period, interval)
        //windowPreprocess(preprocessor, size, period)	
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

