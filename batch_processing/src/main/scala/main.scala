package bitfluc

import sys.process._
import java.net.URL
import java.io.File
import org.apache.spark.sql.SparkSession
import dataload.DataLoader

object BitFluc
{
    def main(args: Array[String])
    {   
        val spark = getSparkSession()
        val dataLoader = new DataLoader(spark)

        val inputPath = "https://storage.googleapis.com/gary_bitcoin_test/bitcoin_data-000000000000.avro"
        //val inputPath = "s3a://gary-reddit-json/comments/comments_2011_05.json"
        val outputPath = "s3a://gary-reddit-parquet/comments"

        //dataLoader.loadURL(url)
        
        //dataLoader.showContent()
        //dataLoader.loadJson(inputPath) 
        dataLoader.loadAvro(inputPath)
        dataLoader.printSchema()
        /*dataLoader.showContent()
        dataLoader.writeParquet(outputPath)*/                                
    }

    def getSparkSession(): SparkSession =  
    {
	val spark = SparkSession.builder
          .appName("Bit Fluc")
          .getOrCreate()
	 
	return spark 
    }
}


