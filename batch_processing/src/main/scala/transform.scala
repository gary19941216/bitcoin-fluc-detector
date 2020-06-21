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
import etl.ETL

import org.apache.spark.sql.streaming._

// transform data to desired format
class Transform(val spark: SparkSession, val dbconnect: DBConnector)
{

    // Join reddit and bitcoin data by window feature and write into Cassandra.
    def windowJoin(reddit_comment: DataFrame, bitcoin_price: DataFrame, threshold: Double, windowSize: Int, period: String, dbtime: String, isSentiment: String, subreddit: String): Unit =
    {

        // add spike column to data
        val bitcoin_price_window = windowProcess(bitcoin_price, windowSize, threshold, "price")
        val reddit_comment_window = windowProcess(reddit_comment, windowSize, threshold, "score")

        // filter out data only with spike exist
        val bitcoin_price_window_spike = bitcoin_price_window.filter((col("spike") === "up" || col("spike") === "down"))
        val reddit_comment_window_spike = reddit_comment_window.filter((col("spike") === "up" || col("spike") === "down"))

        // count all bitcoin spike
        val bitcoin_spike_count = bitcoin_price_window_spike.count()
        // count only bitcoin spike affected by reddit spike
        val affectedSpikeCount = getAffectedSpikeCount(reddit_comment_window_spike, bitcoin_price_window_spike, period)

        println(bitcoin_spike_count)
        println(affectedSpikeCount)

        // import spark.implicits._ for toDF function
        import spark.implicits._

        // turn sequence into dataframe
        val spikeDF = Seq((dbtime, subreddit, isSentiment, bitcoin_spike_count, affectedSpikeCount))
                      .toDF("interval", "subreddit", "nlp", "bitcoin_spike", "bitcoin_reddit_spike")

        // write spike dataframe to cassandra
        val tableName = "spike"
        val keySpace = "bitcoin_reddit"
        dbconnect.writeToCassandra(spikeDF, tableName, keySpace)

        println(dbtime + " " + subreddit + " " + isSentiment)
    } 

    // Join reddit and bitcoin data by time and write into Cassandra.
    // return unjoined reddit and bitcoin aggregated data
    def timeJoin(reddit_comment: DataFrame, bitcoin_price: DataFrame, period: String, interval: Int, dbtime: String, isSentiment: String, subreddit: String): (DataFrame, DataFrame) =
    {
        // get bitcoin price data after averaging the price and persist it
        val bitcoin_price_time = priceInInterval(bitcoin_price, period, interval).persist()

        // initialize with empty dataframe
        var reddit_comment_time = spark.emptyDataFrame
        
        // get reddit score data after being summed which calculate with sentiment and persist it
        if(isSentiment == "with_nlp"){
            reddit_comment_time = nlpScoreInInterval(reddit_comment, period, interval).persist()
        // get reddit score data after being summed which calculate without sentiment and persist it
        } else {
            reddit_comment_time = scoreInInterval(reddit_comment, period, interval).persist()
        }

        // create temp view for reddit_comment_time
        reddit_comment_time.createOrReplaceTempView("reddit_comment_time")
        // create temp view for bitcoin_price_time
        bitcoin_price_time.createOrReplaceTempView("bitcoin_price_time")

        // join bitcoin and reddit data on period
        val bitcoin_reddit_join = joinBitcoinReddit(period)

        // write joined data into cassandra
        val tableName = dbtime + "_" + subreddit + "_" + isSentiment
        val keyspace = "bitcoin_reddit"
        dbconnect.writeToCassandra(bitcoin_reddit_join, tableName, keyspace)

        // return unjoined reddit and bitcoin aggregated data
        (reddit_comment_time, bitcoin_price_time)
    }

    // get the count of bitcoin spike which are affected by Reddit
    def getAffectedSpikeCount(reddit_comment_window_spike: DataFrame, bitcoin_price_window_spike: DataFrame, period: String): Long =
    {
        // initialize Long type
        var affectedSpikeCount: Long = 0

        // if period equals to date
        if(period == "date"){
            affectedSpikeCount = getAffectedSpikeCountDate(reddit_comment_window_spike, bitcoin_price_window_spike)
        // if period equals to date,hour
        } else {
	    affectedSpikeCount = getAffectedSpikeCountDateHour(reddit_comment_window_spike, bitcoin_price_window_spike)
        }

        affectedSpikeCount
    }

    // get affected spike count for period of date and hour
    def getAffectedSpikeCountDateHour(reddit_comment_window_spike: DataFrame, bitcoin_price_window_spike: DataFrame): Long =
    {

	// combine date and hour to new date string
        val combineDateHour = (date: String, hour: Int) => {
            val newDate = date + " " + hour.toString
            newDate
        }

        // user defined function
        val transformDateHour = udf(combineDateHour)

        // add new column date_hour with combined date hour string type
        val reddit_comment_window_spike_date_hour = reddit_comment_window_spike.withColumn("date_hour", transformDateHour(col("date"), col("hour")))
        val bitcoin_price_window_spike_date_hour = bitcoin_price_window_spike.withColumn("date_hour", transformDateHour(col("date"), col("hour")))

        // add new column one_step_after and two_step_after with the date one hour after and two hour after the current date
        val reddit_comment_window_spike_lag = reddit_comment_window_spike_date_hour.withColumn("one_step_after", col("date_hour") + expr("INTERVAL 1 HOUR"))
                                                                                     .withColumn("two_step_after", col("date_hour") + expr("INTERVAL 2 HOUR"))
        
	// create temp view for reddit_comment_window_spike_lag
        reddit_comment_window_spike_lag.createOrReplaceTempView("reddit_comment_window_spike_lag")
        // create temp view for bitcoin_price_window_spike_date_hour
        bitcoin_price_window_spike_date_hour.createOrReplaceTempView("bitcoin_price_window_spike_date_hour")

        // join reddit and bitcoin if reddit spike is on the same day and hour as bitcoin spike or just 1 hour or 2 hour before bitcoin spike
        val affectedSpikedate = spark.sql("""
                                      SELECT DISTINCT(BPWS.date_hour) FROM bitcoin_price_window_spike_date_hour AS BPWS
                                      JOIN reddit_comment_window_spike_lag AS RCWSL
                                      ON RCWSL.date_hour=BPWS.date_hour OR RCWSL.one_step_after=BPWS.date_hour OR RCWSL.two_step_after=BPWS.date_hour
                                      """)
	
	affectedSpikedate.count()
    }

    // get affected spike count for period of date
    def getAffectedSpikeCountDate(reddit_comment_window_spike: DataFrame, bitcoin_price_window_spike: DataFrame): Long = 
    {

	// add new column one_step_after with the date one day after the current date  
        val reddit_comment_window_spike_lag = reddit_comment_window_spike.withColumn("one_step_after", date_add(col("date"), 1))

        // create temp view for reddit_comment_window_spike_lag
        reddit_comment_window_spike_lag.createOrReplaceTempView("reddit_comment_window_spike_lag")

        // create temp view for bitcoin_price_window_spike
        bitcoin_price_window_spike.createOrReplaceTempView("bitcoin_price_window_spike")

        // join reddit and bitcoin if reddit spike is on the same day as bitcoin spike or just 1 day before bitcoin spike
        val affectedSpikedate = spark.sql("""
                                      SELECT DISTINCT(BPWS.date) FROM bitcoin_price_window_spike AS BPWS
                                      JOIN reddit_comment_window_spike_lag AS RCWSL
                                      ON RCWSL.date=BPWS.date OR RCWSL.one_step_after=BPWS.date
	    	    	              """)

        affectedSpikedate.count()
    }

    // bitcoin price in a time interval averaged by period
    // e.g. interval = "365", period = "date, hour"
    def priceInInterval(data: DataFrame, period: String, interval: Int): DataFrame =
    {
        // create temp view for bitcoin_price
        data.createOrReplaceTempView("bitcoin_price")

        // average the price before 2019-11-01 and after (2019-11-01 - Interval)
        // order the data ascending by period
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
    // e.g. interval = "365", period = "date, hour"
    def scoreInInterval(data: DataFrame, period: String, interval: Int): DataFrame =
    {
        // create temp view for reddit_comment_score
        data.createOrReplaceTempView("reddit_comment_score")

        // sum the score before 2019-11-01 and after (2019-11-01 - Interval)    
        // order the data ascending by period
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
    // e.g. interval = "365", period = "date, hour"
    def nlpScoreInInterval(data: DataFrame, period: String, interval: Int): DataFrame =
    {
        // create temp view for reddit_comment_sentiment_score
        data.createOrReplaceTempView("reddit_comment_sentiment_score")

        // sum the sentiment score before 2019-11-01 and after (2019-11-01 - Interval)
        // order the data ascending by period
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
    def joinBitcoinReddit(period: String): DataFrame =
    {
        // if period equals date join on date
        if(period == "date"){
            val joinData = spark.sql("""
            SELECT BP.date, RC.score, BP.price
            FROM reddit_comment_time AS RC
            JOIN bitcoin_price_time AS BP ON RC.date=BP.date
            """)

            joinData
        // if period equals date join on date and hour
        } else {
            val joinData = spark.sql("""
            SELECT BP.date, BP.hour, RC.score, BP.price
            FROM reddit_comment_time AS RC
            JOIN bitcoin_price_time AS BP ON RC.date=BP.date AND RC.hour=BP.hour
            """)

            joinData
        }
    }

    // add spike column if the difference of price or score is bigger than the threshold
    def addSpike(data: DataFrame, threshold: Double, column: String): DataFrame =
    {
        val spike = (value: Double, max: Double, min: Double) => {
            // calculate the difference between current value and window_max, window_min
            var (maxDiff,minDiff) = (max-value, value-min)
            // if value equal 0 return no to prevent divide error with 0
            if(value == 0){
                "no"
            } else {
                // calculate the different ratio for max and min
                var (maxDiffRatio, minDiffRatio) = (maxDiff/value, minDiff/value)
                // if max different ratio is bigger than threshold return up
                if(maxDiffRatio >= threshold){
                    "up"
                // if min different ratio is bigger than threshold return down
                } else if(minDiffRatio >= threshold){
                    "down"
                // // if both different ratio is smaller than threshold return no
                } else {
                    "no"
                }
            }
        }

        // user defined function
        val transform = udf(spike)

        // add spike column using transfrom function on current value, window_max, and window_min
        val spikeData = data.withColumn("spike", transform(col(column),col("window_max"),col("window_min")))

        spikeData
    }

    // adding column retrieved from window function
    def windowProcess(data: DataFrame, size: Int, threshold: Double, column: String): DataFrame =
    {
        // create spark window start from current row to (current+size) row
        val window = Window.rowsBetween(0, size)

        // add window_max column with the max price or score within the window
        // add window_min column with the min price or score within the window
        val windowData = data.withColumn("window_max", max(data(column)).over(window))
                             .withColumn("window_min", min(data(column)).over(window))

        // add spike column
        val spikeData = addSpike(windowData, threshold, column)

        spikeData.show(50)

        spikeData
    }

    // filter by specific subreddit
    def filterSubreddit(data: DataFrame, subreddit: String) : DataFrame =
    {
        // create temp view for reddit comment
        data.createOrReplaceTempView("reddit_comment")

        // select all subreddit
	if(subreddit == "all"){
	    data
        // select from bitcoin, cryptocurrency, ethereum, ripple related subreddit
        } else if(subreddit == "all_below"){
            spark.sql("""
                      SELECT * FROM reddit_comment
                      WHERE LOWER(subreddit) LIKE "%bitcoin%"
                      OR LOWER(subreddit) LIKE "%cryptocurrency%"
                      OR LOWER(subreddit) LIKE "%ethereum%"
                      OR LOWER(subreddit) LIKE "%ripple%"
                      """)
        // select from given subreddit
        } else {
            spark.sql(s"""
                      SELECT * FROM reddit_comment
                      WHERE LOWER(subreddit) LIKE "%${subreddit}%"
                      """)
        }
    }

}
