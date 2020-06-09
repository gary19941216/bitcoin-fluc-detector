package preprocess

import dataload.DataLoader
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

class Preprocessor(val dataloader : DataLoader)
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
        dataloader.updateData(dataloader.getData()
                              .withColumn("time", from_utc_timestamp(from_unixtime(col("created_utc")), "PDT")))
    }

    // seperate PDT time to smaller component
    def seperatePDT() : Unit = 
    {
        dataloader.updateData(dataloader.getData()
                              .withColumn("year", year(col("time")))
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
       dataloader.getData().createOrReplaceTempView("reddit_comment")
       dataloader.updateData(dataloader.spark.sql("""
                              SELECT * FROM reddit_comment
                              WHERE LOWER(subreddit) LIKE "%bitcoin%"
                                 OR LOWER(subreddit) LIKE "%cryptocurrency%"
				 OR LOWER(subreddit) LIKE "%blockchain%"
                              """))
    }

    // filter by specified keyword
    def filterKeyword() : Unit = 
    {
        dataloader.getData().createOrReplaceTempView("reddit_comment")
        //dataloader.updateData(dataloader.getData()
        //                      .filter(col("body").like("%bitcoin%")))
        dataloader.updateData(dataloader.spark.sql("""
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
        dataloader.updateData(dataloader.getData()
                              .filter(length(col("body")) > 0))
    }
}
