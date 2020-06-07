package preprocess

import dataload.DataLoader
import org.apache.spark.sql._

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
        dataloader.updateData(dataloader.getData().withColumn("time", from_utc_timestamp(from_unixtime(col("created_utc")), "PDT")))
    }

    // create year, month, hour from PDT time
    def createYearMonthHour() : Unit = 
    {
        dataloader.updateData(dataloader.getData().withColumn("year", year(col("time"))).withColumn("month", month(col("time"))).withColumn("hour", hour(col("time"))) 
    }
}
