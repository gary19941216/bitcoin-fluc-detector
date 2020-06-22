package preprocess

import com.johnsnowlabs.nlp.pretrained.PretrainedPipeline
import com.johnsnowlabs.nlp.SparkNLP
import org.apache.spark.sql.functions.udf
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql._
import org.apache.spark.sql.functions._
import dataload.DataLoader

// preprocessor for dataloader
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

    // load csv file and upload it back to s3 after transform to Parquet
    def transformCsvToJson(inputPath: String, outputPath: String) : Unit = 
    {
        dataloader.loadCsv(inputPath).writeJson(outputPath)
    }

    // convert unix time to PDT
    def convertUnixToPDT() : Unit = 
    {
        // get data in data loader
        val df = dataloader.getData()

        // convert unix time to PDT time
        dataloader.updateData(df.withColumn("time", from_utc_timestamp(from_unixtime(col("created_utc")), "PDT")))
    }

    // seperate PDT time to date, hour, minute, second
    def seperatePDT() : Unit = 
    {
        // get data in data loader
        val df = dataloader.getData()

        // seperate PDT time to date, hour, minute, second
        dataloader.updateData(df.withColumn("date", to_date(col("time")))
                                .withColumn("hour", hour(col("time")))
                                .withColumn("minute", minute(col("time")))
                                .withColumn("second", second(col("time")))) 
    }

    // select target column for reddit
    def rcSelectColumn() : Unit = 
    {
        // get data in data loader
        val df = dataloader.getData()

        // change created_utc column to timestamp and select are target column
        dataloader.updateData(df.select(col("created_utc").alias("timestamp"),
                              col("date"), col("hour"), col("minute"), col("second"),
                              col("author"), col("subreddit"), col("body").alias("text"), col("score")))
    }

    // select target column for bitcoin
    def bpSelectColumn() : Unit =
    {
        // get data in data loader
        val df = dataloader.getData()

        // change created_utc column to timestamp and select are target column
        dataloader.updateData(df.select(col("created_utc").alias("timestamp"),
                              col("date"), col("hour"), col("minute"), col("second"), col("price")))
    }

    // filter by specified keyword
    def filterKeyword() : Unit = 
    {
        // get data in data loader
        val df = dataloader.getData()

        // create temp view reddit_comment
        df.createOrReplaceTempView("reddit_comment")

        // select reddit comment that at the same time contain "bitcoin" and "price" keyword
        dataloader.updateData(spark.sql("""
                              SELECT * FROM reddit_comment
                              WHERE LOWER(text) LIKE "%bitcoin%"
                                AND LOWER(text) LIKE "%price%"
			      """)) 
    }

    // remove empty body
    def removeEmpty(): Unit = 
    {
        // get data in data loader
        val df = dataloader.getData()

        // update data with new dataframe
        dataloader.updateData(df.filter(length(col("text")) > 0))
    }

    // remove comment post from deleted account
    def removeDeletedAccount(): Unit = 
    {
        // get data in data loader
        val df = dataloader.getData()

        // update data with new dataframe
        dataloader.updateData(df.filter(col("author") =!= "[deleted]"))
    }

    // remove comment with negative score
    def removeNegativeComment(): Unit = 
    {
        // get data in data loader
        val df = dataloader.getData()

        // update data with new dataframe
        dataloader.updateData(df.filter(col("score") > 0))
    }

    // remove bitcoin price data that is higher than 20000
    def removeInvalidData(): Unit = 
    {
        // get data in data loader
        val df = dataloader.getData()

        // update data with new dataframe
        dataloader.updateData(df.filter(col("price") < 20000))
    }

    // add sentiment column
    def addSentiment(sentiment: PretrainedPipeline): Unit =
    {
        // get data in data loader
        val df = dataloader.getData()

        // use nlp PretrainedPipeline transform the dataframe with a new column sentimnet.result
        // select target column from the new dataframe
        dataloader.updateData(sentiment.transform(df)
                              .select(col("timestamp"), 
                              col("date"), col("hour"), col("minute"), col("second"),
                              col("author"), col("subreddit"), col("text"), col("score"),
                              // explode the sentiment.result column to new column sentiment_result
                              explode(col("sentiment.result")).as("sentiment_result")))
    }

    // transfrom sentiment result from str to num
    def sentimentToNum(): Unit = 
    {
        // transfrom sentiment result from str to num
        val transformToScore = (sentiment_result: String) => {
            if(sentiment_result == "positive"){
                1
            } else if(sentiment_result == "negative"){
                -1
            } else {
                0
            }
        }

        // user define function
        val sentimentUDF  = udf(transformToScore)

        // get data in dataloader
        val df = dataloader.getData()

        // update the dataframe with a new column sentiment_score
        dataloader.updateData(df.withColumn("sentiment_score", sentimentUDF(col("sentiment_result"))))
    }

}
