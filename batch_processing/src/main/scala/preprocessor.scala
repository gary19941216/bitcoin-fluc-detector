package preprocess

import dataload.DataLoader
import org.apache.spark.sql._

class Preprocessor(val dataloader : DataLoader)
{

    // load csv file and upload it back to s3 after transform to Parquet
    def transformCsvToParquet(inputPath : String, outputPath : String) : Unit =
    {
        dataloader.loadCsv(inputPath).writeParquet(outputPath)    
    }

    // load json file and upload it back to s3 after transform to Parquet
    def transformJsonToParquet(inputPath : String, outputPath : String) : Unit =
    {
        dataloader.loadJson(inputPath).writeParquet(outputPath)
    }

    //def convertUnixToPDT() :
}
