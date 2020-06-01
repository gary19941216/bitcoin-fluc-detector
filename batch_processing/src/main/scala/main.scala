package bitfluc

import org.apache.spark.sql.SparkSession
import dataload.DataLoader

object BitFluc
{
    def main(args: Array[String])
    {   
        val spark = getSparkSession()
        val dataLoader = new DataLoader(spark)

        val inputPath = "s3a://gary-reddit-json/comments/comments_2011_05.json"
        val outputPath = "s3a://gary-reddit-parquet/comments"

        dataLoader.load(inputPath)
        dataLoader.printSchema()
        dataLoader.showContent()
        dataLoader.writeParquet(outputPath)                                
    }

    def getSparkSession(): SparkSession =  
    {
	val spark = SparkSession.builder
          .appName("Bit Fluc")
          .config("spark.serializer", "org.apache.spark.serializer.KryoSerializer")
	  .config("com.amazonaws.services.s3a.enableV4","true")
	  .config("spark.driver.extraJavaOptions", "-Dcom.amazonaws.services.s3.enableV4=true")
          .config("spark.executor.extraJavaOptions", "-Dcom.amazonaws.servises.s3.enableV4=true")
	  .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
          .config("spark.hadoop.fs.s3a.access.key",sys.env.getOrElse("AWS_ACCESS_KEY_ID", ""))
          .config("spark.hadoop.fs.s3a.secret.key",sys.env.getOrElse("AWS_SECRET_ACCESS_KEY", ""))
          .config("spark.hadoop.fs.s3a.endpoint","s3.us-west-2.amazonaws.com")
	  .config("spark.hadoop.fs.s3a.multiobjectdelete.enable","false")
          .config("spark.hadoop.fs.s3a.fast.upload","true")
          .config("spark.sql.parquet.filterPushdown", "true")
          .config("spark.sql.parquet.mergeSchema", "false")
          .config("spark.hadoop.mapreduce.fileoutputcommitter.algorithm.version", "2")
          .getOrCreate()
	 
	return spark 
    }
}


