package transform

import java.sql.Timestamp
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
import bitfluc.BitFluc

import org.apache.spark.sql.streaming._

object Transform
{
   	
    // Join reddit and bitcoin data by window feature and write into Cassandra.
    def windowJoin(spark: SparkSession, dbconnect: DBConnector, reddit_comment: DataFrame, bitcoin_price: DataFrame, threshold: Double, windowSize: Int, period: String, dbtime: String, isSentiment: String, subreddit: String): Unit =
    {

        val bitcoin_price_window = windowProcess(spark, bitcoin_price, windowSize, threshold, "price")
        val reddit_comment_window = windowProcess(spark, reddit_comment, windowSize, threshold, "score")

        val bitcoin_price_window_spike = bitcoin_price_window.filter((col("spike") === "up" || col("spike") === "down"))
        val reddit_comment_window_spike = reddit_comment_window.filter((col("spike") === "up" || col("spike") === "down"))

        val bitcoin_spike_count = bitcoin_price_window_spike.count()

        var affectedSpikedate = spark.emptyDataFrame 

        bitcoin_price_window_spike.createOrReplaceTempView("bitcoin_price_window_spike")

        if(period == "date"){
            val reddit_comment_window_spike_lag = reddit_comment_window_spike.withColumn("one_step_after", date_add(col("date"), 1))
            reddit_comment_window_spike_lag.createOrReplaceTempView("reddit_comment_window_spike_lag")
            bitcoin_price_window_spike.createOrReplaceTempView("bitcoin_price_window_spike")
		
	    affectedSpikedate = spark.sql("""
                                          SELECT DISTINCT(BPWS.date) FROM bitcoin_price_window_spike AS BPWS
                                          JOIN reddit_comment_window_spike_lag AS RCWSL
                                          ON RCWSL.date=BPWS.date OR RCWSL.one_step_after=BPWS.date
                                          """)
        } else {

            val combineDateHour = (date: String, hour: Int) => {
                val newDate = date + " " + hour.toString
                newDate
            }

            val transformDateHour = udf(combineDateHour)

            val reddit_comment_window_spike_date_hour = reddit_comment_window_spike.withColumn("date_hour", transformDateHour(col("date"), col("hour")))
            val bitcoin_price_window_spike_date_hour = bitcoin_price_window_spike.withColumn("date_hour", transformDateHour(col("date"), col("hour")))

            val reddit_comment_window_spike_lag = reddit_comment_window_spike_date_hour.withColumn("one_step_after", col("date_hour") + expr("INTERVAL 1 HOUR"))
                                                                                     .withColumn("two_step_after", col("date_hour") + expr("INTERVAL 2 HOUR"))
            reddit_comment_window_spike_lag.createOrReplaceTempView("reddit_comment_window_spike_lag")
            bitcoin_price_window_spike_date_hour.createOrReplaceTempView("bitcoin_price_window_spike_date_hour")

	    affectedSpikedate = spark.sql("""
                                          SELECT DISTINCT(BPWS.date_hour) FROM bitcoin_price_window_spike_date_hour AS BPWS
                                          JOIN reddit_comment_window_spike_lag AS RCWSL
                                          ON RCWSL.date_hour=BPWS.date_hour OR RCWSL.one_step_after=BPWS.date_hour OR RCWSL.two_step_after=BPWS.date_hour
                                          """)
        }

        val affectedSpikeCount = affectedSpikedate.count()

        print(bitcoin_spike_count)
        print(affectedSpikeCount)

        import spark.implicits._

        val spikeDF = Seq((dbtime, subreddit, isSentiment, bitcoin_spike_count, affectedSpikeCount))
                      .toDF("interval", "subreddit", "nlp", "bitcoin_spike", "bitcoin_reddit_spike")

        dbconnect.writeToCassandra(spikeDF, "spike", "bitcoin_reddit")

        println(dbtime + " " + subreddit + " " + isSentiment)
    } 

    // Join reddit and bitcoin data by time and write into Cassandra.
    def timeJoin(spark: SparkSession, dbconnect: DBConnector, reddit_comment: DataFrame, bitcoin_price: DataFrame, period: String, interval: Int, dbtime: String, isSentiment: String, subreddit: String): (DataFrame, DataFrame) =
    {
        var reddit_comment_time = spark.emptyDataFrame
        if(isSentiment == "with_nlp"){
            reddit_comment_time = nlpScoreInInterval(spark, reddit_comment, period, interval).persist()
        } else {
            reddit_comment_time = scoreInInterval(spark, reddit_comment, period, interval).persist()
        }

        val bitcoin_price_time = priceInInterval(spark, bitcoin_price, period, interval).persist()

        reddit_comment_time.createOrReplaceTempView("reddit_comment_time")
        bitcoin_price_time.createOrReplaceTempView("bitcoin_price_time")

        val bitcoin_reddit_join = joinBitcoinReddit(spark, period)

        dbconnect.writeToCassandra(bitcoin_reddit_join, dbtime + "_" + subreddit + "_" + isSentiment, "bitcoin_reddit")

        (reddit_comment_time, bitcoin_price_time)
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
                  FROM reddit_comment_score
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
        } else {
            val joinData = spark.sql("""
            SELECT BP.date, BP.hour, RC.score, BP.price
            FROM reddit_comment_time AS RC
            JOIN bitcoin_price_time AS BP ON RC.date=BP.date AND RC.hour=BP.hour
            """)

            joinData
        }
    }

    // add spike column
    def addSpike(spark: SparkSession, data: DataFrame, threshold: Double, column: String): DataFrame =
    {
        val spike = (value: Int, max: Int, min: Int) => {
            var (maxDiff,minDiff) = (max-value, value-min)
            if(value == 0){
                "no"
            } else {
                var (maxDiffRatio, minDiffRatio) = (maxDiff/value, minDiff/value)
                if(maxDiffRatio >= threshold){
                    "up"
                } else if(minDiffRatio >= threshold){
                    "down"
                } else {
                    "no"
                }
            }
        }

        val transform = udf(spike)

        val spikeData = data.withColumn("spike", transform(col(column),col("window_max"),col("window_min")))

        spikeData
    }

    // adding column retrieved from window function
    def windowProcess(spark: SparkSession, data: DataFrame, size: Int, threshold: Double, column: String): DataFrame =
    {
        val window = Window.orderBy("date").rowsBetween(0, size)
        val windowData = data.withColumn("window_max", max(data(column)).over(window))
                             .withColumn("window_min", min(data(column)).over(window))
        val spikeData = addSpike(spark, windowData, threshold, column)

        spikeData
    }

    // filter by specific subreddit
    def filterSubreddit(spark: SparkSession, data: DataFrame, subreddit: String) : DataFrame =
    {
        data.createOrReplaceTempView("reddit_comment")

        if(subreddit == "all"){
            spark.sql("""
                      SELECT * FROM reddit_comment
                      WHERE LOWER(subreddit) LIKE "%bitcoin%"
                      OR LOWER(subreddit) LIKE "%cryptocurrency%"
                      OR LOWER(subreddit) LIKE "%ethereum%"
                      OR LOWER(subreddit) LIKE "%tether%"
                      """)
        } else {
            spark.sql(s"""
                      SELECT * FROM reddit_comment
                      WHERE LOWER(subreddit) LIKE "%${subreddit}%"
                      """)
        }
    }    
}
