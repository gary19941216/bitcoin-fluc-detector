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

	rcloadPreprocess(rcPreprocessor, rcParquetPath, "parquet", sentiment)
	bploadPreprocess(bpPreprocessor, bpCsvPath, "csv")

	val timeList = List(("date", 3650, "ten_year", 1, 0.1, "1day")
                            ,("date", 1825, "five_year", 1, 0.1, "1day")
                            ,("date", 1095, "three_year", 1, 0.1, "1day")
                            ,("date", 365, "one_year", 1, 0.1, "1day")
                            ,("date,hour", 180, "six_month", 3, 0.04, "1hour")
                            ,("date,hour", 90, "three_month", 3, 0.04, "1hour")
                            ,("date,hour", 30, "one_month", 3, 0.04, "1hour")
                            ,("date,hour,minute", 5, "five_day", 20, 0.02, "1min"))

        val reddit_comment = rcLoader.getData()
        reddit_comment.explain()
	val bitcoin_price = bpLoader.getData()
        reddit_comment.persist()
        bitcoin_price.persist()
  
	for((period, interval, dbtime, windowSize, threshold, tolerance) <- timeList){
            val (reddit_comment_time, bitcoin_price_time) = timeJoin(spark, dbconnect,reddit_comment, bitcoin_price, period, interval, dbtime)
            windowJoin(spark, dbconnect,reddit_comment_time, bitcoin_price_time, threshold, windowSize, tolerance)
	}

        println("Finish Writing!!!!!!!!!!!!!!!!!!!!")
    } 
    
    // Join reddit and bitcoin data by window feature and write into Cassandra.
    def windowJoin(spark: SparkSession, dbconnect: DBConnector, reddit_comment: DataFrame, bitcoin_price: DataFrame, threshold: Double, windowSize: Int, tolerance: String): Unit = 
    {
        val bitcoin_price_window = windowProcess(spark, bitcoin_price, windowSize, threshold, "price")
        val reddit_comment_window = windowProcess(spark, reddit_comment, windowSize, threshold, "score")

        val bitcoin_price_window_spike = bitcoin_price_window.filter((col("spike") === "up" || col("spike") === "down"))
        val reddit_comment_window_spike = reddit_comment_window.filter((col("spike") === "up" || col("spike") === "down"))

        val reddit_tsRDD = TimeSeriesRDD.fromDF(dataFrame = reddit_comment_window_spike
                                                .select("time","score"))(isSorted = true, timeUnit = MILLISECONDS)

        val bitcoin_tsRDD = TimeSeriesRDD.fromDF(dataFrame = bitcoin_price_window_spike
                                                 .select("time","price"))(isSorted = true, timeUnit = MILLISECONDS)

        val reddit_bitcoin_spike = flintLeftJoin(reddit_tsRDD, bitcoin_tsRDD, tolerance) 

        val bitcoin_spike_count = bitcoin_price_window_spike.count()
        val reddit_spike_count = reddit_comment_window_spike.count()
        val reddit_bitcoin_spike_count = reddit_bitcoin_spike.count()
    }

    // Join reddit and bitcoin data by time and write into Cassandra.
    def timeJoin(spark: SparkSession, dbconnect: DBConnector, reddit_comment: DataFrame, bitcoin_price: DataFrame, period: String, interval: Int, dbtime: String): (DataFrame, DataFrame) = 
    {
        val reddit_comment_time = scoreInInterval(spark, reddit_comment, period, interval)
        val bitcoin_price_time = priceInInterval(spark, bitcoin_price, period, interval)

        reddit_comment_time.createOrReplaceTempView("reddit_comment_time")
        bitcoin_price_time.createOrReplaceTempView("bitcoin_price_time")

        val time_body_price = joinBitcoinReddit(spark, period)

        dbconnect.writeToCassandra(time_body_price, "reddit_bitcoin_" + dbtime + "_all_no_sentiment", "bitcoin_reddit")

        (reddit_comment_time, bitcoin_price_time)
    }

    // left join reddit_comment spike on bitcoin_price spike if reddit_commment date is same or before bitcoin_price date
    def flintLeftJoin(bitcoin_price: TimeSeriesRDD, reddit_comment: TimeSeriesRDD, tolerance: String): TimeSeriesRDD =
    {
        // e.g. tolerance='1day'
        //flint_df = flintContext.read.dataframe(df)
        val asOfJoinDF = bitcoin_price.leftJoin(reddit_comment, tolerance=tolerance)

        asOfJoinDF
    }

    // bitcoin price in a time interval averaged by period
    def priceInInterval(spark: SparkSession, data: DataFrame, period: String, interval: Int): DataFrame =
    {
        data.createOrReplaceTempView("bitcoin_price")

        val priceData = spark.sql(s"""
                  SELECT ${period}, ROUND(AVG(price),2) AS price
                  FROM bitcoin_price
                  WHERE date >= DATE_ADD(CAST('2019-11-01' AS DATE), ${-interval})
                  AND date < DATE_ADD(CAST('2019-11-01' AS DATE), 0)
                  GROUP BY ${period}
                  ORDER BY ${period}
                  """)

	priceData
    }

    // reddit comment score in a time interval aggregated by period
    def scoreInInterval(spark: SparkSession, data: DataFrame, period: String, interval: Int): DataFrame =
    {
        // e.g. interval = "365", period = "date, hour"
        data.createOrReplaceTempView("reddit_comment_score")

        val scoreData = spark.sql(s"""
                  SELECT ${period}, SUM(score) AS score
                  FROM reddit_comment
                  WHERE date >= DATE_ADD(CAST('2019-11-01' AS DATE), ${-interval})
                  AND date < DATE_ADD(CAST('2019-11-01' AS DATE), 0)
                  GROUP BY ${period}
                  ORDER BY ${period}
                  """)

	scoreData
    }

    // reddit comment score with sentiment in a time interval aggregated by period
    def nlpScoreInInterval(spark: SparkSession, data: DataFrame, period: String, interval: Int): DataFrame =
    {
        // e.g. interval = "365", period = "date, hour"
        data.createOrReplaceTempView("reddit_comment_sentiment_score")

        val scoreData = spark.sql(s"""
                  SELECT ${period}, SUM(score*sentiment_score) AS score
                  FROM reddit_comment_sentiment_score
                  WHERE date >= DATE_ADD(CAST('2019-11-01' AS DATE), ${-interval})
                  AND date < DATE_ADD(CAST('2019-11-01' AS DATE), 0)
                  GROUP BY ${period}
                  ORDER BY ${period}
                  """)

        scoreData
    } 

    // load pretrained nlp model. 
    def loadNLPModel(): PretrainedPipeline =
    {
        val sentiment = PretrainedPipeline
        .fromDisk("/usr/local/spark_nlp")

        sentiment
    }

    // join bitcoin and reddit dataset
    def joinBitcoinReddit(spark: SparkSession, period: String): DataFrame =
    {
	if(period == "date"){
	    val joinData = spark.sql("""
            SELECT BP.date, RC.score, BP.price
            FROM reddit_comment_time AS RC
            JOIN bitcoin_price_time AS BP ON RC.date=BP.date
            """)

            joinData
        } else if(period == "date,hour"){  
	    val joinData = spark.sql("""
	    SELECT BP.date, BP.hour, RC.score, BP.price
	    FROM reddit_comment_time AS RC
	    JOIN bitcoin_price_time AS BP ON RC.date=BP.date AND RC.hour=BP.hour
	    """)

            joinData
	} else {
	    val joinData = spark.sql("""
            SELECT BP.date, BP.hour, BP.minute, RC.score, BP.price
            FROM reddit_comment_time AS RC
            JOIN bitcoin_price_time AS BP ON RC.date=BP.date AND RC.hour=BP.hour AND RC.minute=BP.minute
            """)

            joinData
	}
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

    // add spike column
    def addSpike(spark: SparkSession, data: DataFrame, threshold: Double, column: String): DataFrame =
    {
        val spike = (price: Int, max: Int, min: Int) => {
            var (maxDiff,minDiff) = (max-price, price-min)
            var (maxDiffRatio, minDiffRatio) = (maxDiff/price, minDiff/price)
            if(maxDiffRatio > threshold){   
                "up"
            } else if(minDiffRatio > threshold){  
                "down"
            } else {
                "no"
            }
        }

        val transform = udf(spike)

        val spikeData = data.withColumn("spike", transform(col(column),col("window_max"),col("window_min")))

	spikeData
    } 

    // adding column retrieved from window function
    def windowProcess(spark: SparkSession, data: DataFrame, size: Int, threshold: Double, column: String): DataFrame =
    {
        val window = Window.rowsBetween(0, size)
        val windowData = data.withColumn("window_max", max(data(column)).over(window))
                             .withColumn("window_min", max(data(column)).over(window))
        val spikeData = addSpike(spark, windowData, threshold, column) 
	
	spikeData
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
        preprocessor.selectColumn()
        bodyPreprocess(preprocessor)
        preprocessor.filterSubreddit()
        preprocessor.removeNegativeComment()
        preprocessor.removeDeletedAccount()
        sentimentPreprocess(preprocessor, sentiment)
        //preprocessor.dataloader.persist()
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
        //preprocessor.dataloader.persist()
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
          //.config("spark.sql.streaming.checkpointLocation", "checkpoint")
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

