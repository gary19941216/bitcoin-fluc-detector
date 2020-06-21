package unify

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
import java.sql.Date
import dataload.DataLoader
import preprocess.Preprocessor
import dbconnector.DBConnector
import scala.concurrent.duration._
import transform.Transform
import etl.ETL

// unify historical and real time data
object Unify
{
    // get spark session
    private val spark = getSparkSession()

    // initialize DBConnector object
    private val dbconnect = new DBConnector(spark)

    // load spark session and dbconnect into Transform object
    private val transform = new Transform(spark, dbconnect)

    // load nlp model
    private val sentiment = ETL.loadNLPModel() 

    def main(args: Array[String])
    {

	// read bitcoin real time data from Cassandra
        val tableName = "bitcoin_streaming_test"
        val keySpace = "bitcoin_reddit"
        val bitcoin_real_time = dbconnect.readFromCassandra(tableName, keySpace)

	// List of different time interval and period
       	val timeList = List(//("date", 3650, "ten_year", 1, 0.05)
                            ("date", 1825, "five_year", 1, 0.1)
                            ,("date", 1095, "three_year", 1, 0.1)
                            ,("date", 365, "one_year", 1, 0.1)
                            ,("date", 180, "six_month", 1, 0.1)
                            ,("date", 90, "three_month", 1, 0.1)
                            ,("date,hour", 30, "one_month", 5, 0.03)
                            ,("date,hour", 5, "five_day", 5, 0.03)) 

        // loop through the time list
	for((period, interval, dbtime, windowSize, threshold) <- timeList){
            
            // target subreddit 
            val subredditList = List("all","all_below","bitcoin","cryptocurrency","ethereum","ripple")

            // loop through subreddit list
            for(subreddit <- subredditList){
                
                // List for using nlp sentiment analysis or not 
                val withSentiment = List("no_nlp", "with_nlp")

                // loop through both of them
                for(isSentiment <- withSentiment){
                 
                    // read reddit real time data from cassandra
                    val tableName = "reddit_streaming_test5_" + subreddit + "_" + isSentiment
                    val keySpace = "bitcoin_reddit"
                    val reddit_real_time = dbconnect.readFromCassandra(tableName, keySpace)
    
                    // get the latest date in reddit data
                    val latestDate = getLatestDate(reddit_real_time)

                    // get historical join data and seperate it into reddit and bitcoin data
                    val (reddit_historical, bitcoin_historical) = getBitcoinReddit(period, dbtime, subreddit, isSentiment, latestDate, interval)

                    // join reddit and bitcoin real time data and write into cassandra 
                    joinDataWriteToDB(reddit_real_time, bitcoin_real_time, period, dbtime, subreddit, isSentiment)

                    // get union data for reddit and bitcoin
                    val (reddit_comment_time, bitcoin_price_time) = getUnionData(period, reddit_real_time, bitcoin_real_time, reddit_historical, bitcoin_historical)

                    // calculate and update the spike count to cassandra
                    transform.windowJoin(reddit_comment_time, bitcoin_price_time, threshold, windowSize, period, dbtime, isSentiment, subreddit)
                }
            }
        }
    }

    // join reddit and bitcoin real time data and write into cassandra
    def joinDataWriteToDB(reddit_real_time: DataFrame, bitcoin_real_time: DataFrame, period: String, dbtime: String, subreddit: String, isSentiment: String): Unit =
    {
       	// sum score for Reddit 
        val reddit_score = sumScore(reddit_real_time, period)

        // average for bitcoin price
        val bitcoin_price = avgPrice(bitcoin_real_time, period)

        // join bitcoin and reddit data
        var bitcoin_reddit_join = spark.emptyDataFrame
        // if period equal to date only join on date column
        if(period == "date"){
            bitcoin_reddit_join = reddit_score.join(bitcoin_price, Seq("date"))
        // if period equal to date, hour join on date and hour column
        } else {
            bitcoin_reddit_join = reddit_score.join(bitcoin_price, Seq("date","hour"))
        }

        // write real time reddit and bitcoin join data to database
        val tableName = dbtime + "_" + subreddit + "_" + isSentiment
        val keyspace = "bitcoin_reddit"
        dbconnect.writeToCassandra(bitcoin_reddit_join, tableName, keyspace) 
    }

    // union real_time and historical dataset
    def getUnionData(period: String, reddit_real_time: DataFrame, bitcoin_real_time: DataFrame, reddit_historical: DataFrame, bitcoin_historical: DataFrame): (DataFrame, DataFrame) = 
    {
       	// initialize empty dataframe for both reddit and bitcoin 
        var reddit_comment_time = spark.emptyDataFrame
        var bitcoin_price_time = spark.emptyDataFrame
        
	//if period equals to date, only select date, score, price column for real time data  
        if(period == "date"){
            reddit_comment_time = reddit_historical.union(reddit_real_time.select("date", "score"))
            bitcoin_price_time = bitcoin_historical.union(bitcoin_real_time.select("date", "price"))
        // if period equals to date,hour , select date, hour, score, price column for real time data 
        } else {
            reddit_comment_time = reddit_historical.union(reddit_real_time.select("date", "hour", "score"))
            bitcoin_price_time = bitcoin_historical.union(bitcoin_real_time.select("date", "hour", "price"))
        }

        (reddit_comment_time, bitcoin_price_time)
    }

    // seperate join data to bitcoin and reddit data
    def getBitcoinReddit(period: String, dbtime: String, subreddit: String, isSentiment: String, date: Date, interval: Int): (DataFrame, DataFrame) =
    {
   
	// read historical data from cassandra
        val bitcoin_reddit_historical = dbconnect.readFromCassandra(dbtime + "_" + subreddit + "_" + isSentiment, "bitcoin_reddit")

        // get data only within the time interval
        val bitcoin_reddit_historical_valid = filterValidData(bitcoin_reddit_historical, date, interval) 

	// initialize empty dataframe
       	var bitcoin_historical = spark.emptyDataFrame
        var reddit_historical = spark.emptyDataFrame
        
        // select column according period
	if(period == "date"){
            bitcoin_historical = bitcoin_reddit_historical_valid.select("date","price")
            reddit_historical = bitcoin_reddit_historical_valid.select("date","score")
        } else {
            bitcoin_historical = bitcoin_reddit_historical_valid.select("date", "hour","price")
            reddit_historical = bitcoin_reddit_historical_valid.select("date", "hour", "score")
        } 
  
        (reddit_historical, bitcoin_historical)
    }

    // filter data within the date
    def filterValidData(data: DataFrame, date: Date, interval: Int): DataFrame =
    {
        // create temp view
        data.createOrReplaceTempView("historical")

        // get data within the latest date and time interval
        val validData  = spark.sql(s"""
                                   SELECT * FROM historical
                                   WHERE date >= DATE_ADD('${date}', ${-interval})
                                   AND date < DATE_ADD('${date}', 0)
                                   """)
        validData
    }

    // get the latest date in data
    def getLatestDate(data: DataFrame): Date =
    {
        // create temp view
        data.createOrReplaceTempView("real_time")

        // get the latest date in real time data, Row type.
        val date = spark.sql("""
                             SELECT MAX(date) AS date 
                             FROM real_time
                             """).first()
       date.getDate(0)
    }

    // sum score for reddit
    def sumScore(data: DataFrame, period: String): DataFrame =
    {
        // create temp view
        data.createOrReplaceTempView("reddit_comment")

        // select time column and calculate score sum
        spark.sql(s"""
                  SELECT ${period}, SUM(score) AS score FROM reddit_comment
                  GROUP BY ${period}
                  """)
    }

    // average price for bitcoin
    def avgPrice(data: DataFrame, period: String): DataFrame =
    {
        // create temp view
        data.createOrReplaceTempView("bitcoin_price")
        
        // select time column and calculate average price
        spark.sql(s"""
                  SELECT ${period}, AVG(price) AS price FROM bitcoin_price                                       
                  GROUP BY ${period}                                      
                  """)
    }

    // get SparkSession with configuration
    def getSparkSession(): SparkSession =
    {
        val spark = SparkSession.builder
          .appName("unify historical and real time data")
          // standalone mode with master ip and port
          .master("spark://10.0.0.11:7077")
          // set parallelism for spark job
          .config("spark.default.parallelism", 50)
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
}
