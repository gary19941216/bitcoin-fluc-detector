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
import dataload.DataLoader
import preprocess.Preprocessor
import dbconnector.DBConnector
import com.twosigma.flint.timeseries._
import scala.concurrent.duration._
import transform.Transform
import bitfluc.BitFluc

object Unify
{
    def main(args: Array[String])
    {
        val spark = getSparkSession()
        val dbconnect = new DBConnector(spark)
        val sentiment = BitFluc.loadNLPModel()

        val bitcoin_real_time = dbconnect.readFromCassandra("bitcoin_streaming_test", "bitcoin_reddit")

       	val timeList = List(("date", 3650, "ten_year", 1, 0.05)
                            ,("date", 1825, "five_year", 1, 0.05)
                            ,("date", 1095, "three_year", 1, 0.05)
                            ,("date", 365, "one_year", 1, 0.05)
                            ,("date", 180, "six_month", 1, 0.05)
                            ,("date", 90, "three_month", 1, 0.05)
                            ,("date,hour", 30, "one_month", 5, 0.008)
                            ,("date,hour", 5, "five_day", 5, 0.008)) 

	for((period, interval, dbtime, windowSize, threshold) <- timeList){
            val subredditList = List("all","bitcoin","cryptocurrency","ethereum","ripple")

            for(subreddit <- subredditList){
                val withSentiment = List("no_nlp", "with_nlp")

                for(isSentiment <- withSentiment){
                 
                    val reddit_real_time = dbconnect.readFromCassandra("reddit_streaming_test_" + subreddit + "_" + isSentiment, "bitcoin_reddit")
                    val latestDate = getLatestDate(spark, reddit_real_time)

                    val bitcoin_reddit_historical_valid = readRewriteToDB(spark, latestDate, interval, dbtime, subreddit, isSentiment, dbconnect)

                    val (reddit_historical, bitcoin_historical) = getBitcoinReddit(spark, bitcoin_reddit_historical_valid, period)

                    var (reddit_comment_time, bitcoin_price_time) = Transform.timeJoin(spark, dbconnect,reddit_real_time,
                                                                                        bitcoin_real_time, period, interval, dbtime, isSentiment, subreddit)

                    reddit_comment_time = reddit_historical.union(reddit_comment_time)
                    bitcoin_price_time = bitcoin_historical.union(bitcoin_price_time)

                    Transform.windowJoin(spark, dbconnect, reddit_comment_time, bitcoin_price_time, threshold, windowSize, period, dbtime, isSentiment, subreddit)
                }
            }
        }

        /*import spark.implicits._

        val someDF = Seq(
            (8, "bat"),
            (64, "mouse"),
            (-27, "horse")
        ).toDF("number", "word")

        print(someDF.count())*/
    }

    // seperate join data to bitcoin and reddit data
    def getBitcoinReddit(spark: SparkSession, data: DataFrame, period: String): (DataFrame, DataFrame) =
    {
       	var bitcoin_historical = spark.emptyDataFrame
        var reddit_historical = spark.emptyDataFrame
        
	if(period == "date"){
            bitcoin_historical = data.select("date","price")
            reddit_historical = data.select("date","score")
        } else {
            bitcoin_historical = data.select("date", "hour","price")
            reddit_historical = data.select("date", "hour", "score")
        } 
  
        (reddit_historical, bitcoin_historical)
    }

    // read data from database and rewrite filtered data back to database
    def readRewriteToDB(spark: SparkSession, date: String, interval: Int, dbtime: String, subreddit: String, isSentiment: String, dbconnect: DBConnector): DataFrame =
    {
        val bitcoin_reddit_historical = dbconnect.readFromCassandra(dbtime + "_" + subreddit + "_" + isSentiment, "bitcoin_reddit")
        
        val bitcoin_reddit_historical_valid = filterValidData(spark, bitcoin_reddit_historical, date, interval)
        
        dbconnect.overwriteCassandra(bitcoin_reddit_historical_valid, dbtime + "_" + subreddit + "_" + isSentiment, "bitcoin_reddit")

        bitcoin_reddit_historical_valid
    }

    // filter data within the date
    def filterValidData(spark: SparkSession, data: DataFrame, date: String, interval: Int): DataFrame =
    {
        data.createOrReplaceTempView("historical")

        val validData  = spark.sql(s"""
                                   SELECT * FROM historical
                                   WHERE date >= DATE_ADD(CAST(${date} AS DATE), ${-interval})
                                   AND date < DATE_ADD(CAST(${date} AS DATE), 0)
                                   """)
        validData
    }

    // get the latest date in data
    def getLatestDate(spark: SparkSession, data: DataFrame): String =
    {
        data.createOrReplaceTempView("real_time")

        val date = spark.sql("""
                             SELECT MAX(date) AS date 
                             FROM real_time
                             """).first()
        date.getString(0)
    }

    // get SparkSession with configuration
    def getSparkSession(): SparkSession =
    {
        val spark = SparkSession.builder
          .appName("test")
          .master("spark://10.0.0.11:7077")
          .config("spark.executor.cores",6)
          .config("spark.default.parallelism", 400)
          .config("spark.cassandra.connection.host", "10.0.0.6")
          .config("spark.cassandra.auth.username", "cassandra")
          .config("spark.cassandra.auth.password", "cassandra")
          .config("spark.cassandra.connection.port","9042")
          .config("spark.cassandra.output.consistency.level","ONE")
          .getOrCreate()

        spark.sparkContext.setLogLevel("ERROR")

        spark
    }
}
