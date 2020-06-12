package preprocess

import dataload.DataLoader
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Preprocessor(val spark: SparkSession, val dataloader : DataLoader)
{

    // load csv file and upload it back to s3 after transform to Parquet
    def transformCsvToParquet(inputPath: String, outputPath: String) : Unit =
    {
        dataloader.loadCsv(inputPath).writeParquet(outputPath)    
    }

    // load json file and upload it back to s3 after transform to Parquet
    def transformJsonToParquet(inputPath: String, outputPath: String) : Unit =
    {
        dataloader.loadJson(inputPath).writeParquet(outputPath)
    }

    // convert unix time to PDT
    def convertUnixToPDT() : Unit = 
    {
        val df = dataloader.getData()
        dataloader.updateData(df.withColumn("time", from_utc_timestamp(from_unixtime(col("created_utc")), "PDT")))
    }

    // seperate PDT time to smaller component
    def seperatePDT() : Unit = 
    {
        val df = dataloader.getData()
        dataloader.updateData(df.withColumn("year", year(col("time")))
                                .withColumn("month", month(col("time")))
                                .withColumn("day", dayofmonth(col("time")))
                                .withColumn("date", to_date(col("time")))
                                .withColumn("hour", hour(col("time")))) 
    }

    // filter by specific subreddit
    def filterSubreddit() : Unit = 
    {
        /*val list = List("CryptoCurrency", "CyptoCurrencyTrading", "CyptoCurrencies") 
        dataloader.updateData(dataloader.getData()
                              .filter(col("subreddit").isin(list:_*)))*/
       val df = dataloader.getData()
       df.createOrReplaceTempView("reddit_comment")
       dataloader.updateData(spark.sql("""
                              SELECT * FROM reddit_comment
                              WHERE LOWER(subreddit) LIKE "%bitcoin%"
                                 OR LOWER(subreddit) LIKE "%cryptocurrency%"
				 OR LOWER(subreddit) LIKE "%blockchain%"
                                 OR LOWER(subreddit) LIKE "%tether%"
                              """))
    }

    // filter by specified keyword
    def filterKeyword() : Unit = 
    {
        val df = dataloader.getData()
        df.createOrReplaceTempView("reddit_comment")
        //dataloader.updateData(dataloader.getData()
        //                      .filter(col("body").like("%bitcoin%")))
        dataloader.updateData(spark.sql("""
                              SELECT * FROM reddit_comment
                              WHERE LOWER(body) LIKE "%bitcoin%"
                                 OR LOWER(body) LIKE "%cryptocurrency%"
				 OR LOWER(body) LIKE "%distributed%ledger%"
				 OR LOWER(body) LIKE "%blockchain%"
                              """))
    }

    // remove empty body
    def removeEmpty(): Unit = 
    {
        val df = dataloader.getData()
        dataloader.updateData(df.filter(length(col("body")) > 0))
    }

    // remove comment post from deleted account
    def removeDeletedAccount(): Unit = 
    {
        val df = dataloader.getData()
        dataloader.updateData(df.filter(col("author") =!= "[deleted]"))
    }

    // remove comment with negative score
    def removeNegativeComment(): Unit = 
    {
        val df = dataloader.getData()
        dataloader.updateData(df.filter(col("score") > 0))
    }

    def removeInvalidData(): Unit = 
    {
        val df = dataloader.getData()
        dataloader.updateData(df.filter(col("price") < 20000))
    }

    // bitcoin price in a time interval averaged by period
    def priceInInterval(period: String, interval: Int): Unit = 
    {
        val df = dataloader.getData()
	df.createOrReplaceTempView("bitcoin_price")
        dataloader.updateData(spark.sql(s"""
                              SELECT ${period}, ROUND(AVG(price),2) AS price, MIN(price) AS min, MAX(price) AS max
                              FROM bitcoin_price
                              WHERE date >= DATE_ADD(CAST('2019-07-01' AS DATE), ${-interval})
                              AND date < DATE_ADD(CAST('2019-07-01' AS DATE), 0)
                              GROUP BY ${period}
                              ORDER BY ${period}
                              """))
    }

    // reddit comment score in a time interval aggregated by period
    def scoreInInterval(period: String, interval: Int): Unit = 
    {
	// e.g. interval = "5 YEAR", period = "YEAR(date), WEEK(date)"
        val df = dataloader.getData()
        df.createOrReplaceTempView("reddit_comment")
        dataloader.updateData(spark.sql(s"""
                              SELECT ${period}, SUM(score) AS score
                              FROM reddit_comment
                              WHERE date >= DATE_ADD(CAST('2019-07-01' AS DATE), ${-interval}) 
                              AND date < DATE_ADD(CAST('2019-07-01' AS DATE), 0)
                              GROUP BY ${period}
                              ORDER BY ${period}
                              """))
    }

    // add window max within the window size, start from the time
    def addWindowMax(size: Int): Unit = 
    {
        val window = Window.rowsBetween(0, size)
        val df = dataloader.getData()
        dataloader.updateData(df.withColumn("window_max", max(df("price")).over(window)))
    }

    // add window max within the window size, start from the time
    def addWindowMin(size: Int): Unit =
    {
        val window = Window.rowsBetween(0, size)
        val df = dataloader.getData()
        dataloader.updateData(df.withColumn("window_min", min(df("price")).over(window)))
    }

    // add spike column
    /*def addSpike(period: Int): Unit = 
    {   
        val spike = (price: Int, max: Int, min: Int) => {
	    var (maxDiff,minDiff) = (max-price, price-min)
            var (maxDiffRatio, minDiffRatio) = (maxDiff/price, minDiff/price)
            if(maxDiffRatio > 0.05){
                "up"
            } else if(minDiffRatio > 0.05){
                "down"
            } else {
                "no"
            }            
        }

        val df = dataloader.getData()
        dataloader.updateData(df.withColumn("spike", spike("price","window_max","window_min")))
    }*/

}
