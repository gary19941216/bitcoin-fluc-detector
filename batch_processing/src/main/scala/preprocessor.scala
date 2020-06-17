package preprocess

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.SparkNLP
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

    def transformCsvToJson(inputPath: String, outputPath: String) : Unit = 
    {
        dataloader.loadCsv(inputPath).writeJson(outputPath)
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
        dataloader.updateData(df.withColumn("date", to_date(col("time")))
                                .withColumn("hour", hour(col("time")))
                                .withColumn("minute", minute(col("time")))
                                .withColumn("second", second(col("time")))) 
    }

    // select target column for reddit
    def rcSelectColumn() : Unit = 
    {
        val df = dataloader.getData()
        dataloader.updateData(df.select(col("created_utc").alias("timestamp"),
                              col("date"), col("hour"), col("minute"), col("second"),
                              col("author"), col("subreddit"), col("body").alias("text"), col("score")))
    }

    // select target column for bitcoin
    def bpSelectColumn() : Unit =
    {
        val df = dataloader.getData()
        dataloader.updateData(df.select(col("created_utc").alias("timestamp"),
                              col("date"), col("hour"), col("minute"), col("second"), col("price")))
    }

    // filter by specific subreddit
    def filterSubreddit() : Unit = 
    {
        /*val list = List("CryptoCurrency","CryptoCurrencyTrading","CryptoCurrencies","Bitcoin") 
        dataloader.updateData(dataloader.getData()
                              .filter(col("subreddit").isin(list:_*)))*/
       val df = dataloader.getData()
       df.createOrReplaceTempView("reddit_comment")
       dataloader.updateData(spark.sql("""
                              SELECT * FROM reddit_comment
                              WHERE LOWER(subreddit) LIKE "%bitcoin%"
                              """))
                                 /*OR LOWER(subreddit) LIKE "%cryptocurrency%"
				 OR LOWER(subreddit) LIKE "%ethereum%"
                                 OR LOWER(subreddit) LIKE "%tether%"
                              """))*/
    }

    // filter by specified keyword
    def filterKeyword() : Unit = 
    {
        val df = dataloader.getData()
        df.createOrReplaceTempView("reddit_comment")
        /*dataloader.updateData(dataloader.getData()
                              .filter(col("body").rlike("bitcoin")))*/
        dataloader.updateData(spark.sql("""
                              SELECT * FROM reddit_comment
                              WHERE LOWER(text) LIKE "%bitcoin%"
                                AND LOWER(text) LIKE "%price%"
			      """)) 
    }

    // remove empty body
    def removeEmpty(): Unit = 
    {
        val df = dataloader.getData()
        dataloader.updateData(df.filter(length(col("text")) > 0))
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

    // add sentiment column
    def addSentiment(sentiment: PretrainedPipeline): Unit =
    {
        val df = dataloader.getData()
        dataloader.updateData(sentiment.transform(df)
                              .select(col("timestamp"), 
                              col("date"), col("hour"), col("minute"), col("second"),
                              col("author"), col("subreddit"), col("text"), col("score"),
                              explode(col("sentiment.result")).as("sentiment_result")))
    }

    def sentimentToNum(): Unit = 
    {
        val transformToScore = (sentiment_result: String) => {
            if(sentiment_result == "positive"){
                1
            } else if(sentiment_result == "negative"){
                -1
            } else {
                0
            }
        }

        val sentimentUDF  = udf(transformToScore)

        val df = dataloader.getData()
        dataloader.updateData(df.withColumn("sentiment_score", sentimentUDF(col("sentiment_result"))))
    }

}
