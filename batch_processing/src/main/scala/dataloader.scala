package dataload

import org.apache.spark.sql._
import org.apache.spark.rdd.RDD

class DataLoader(val spark: SparkSession)
{
    private var data : Dataset[Row] = _  

    //load json data from file
    def loadJson(path: String) : Unit =
    {    
        data = spark.read.json(path)
    }

    //load avro data from file
    def loadAvro(path: String) : Unit = 
    {
        data = spark.read.format("avro").load(path)
    }

    def loadParquet(path: String) : Unit = 
    {
        data = spark.read.parquet(path)
    }

    //retrieve data
    def getData() : Dataset[Row] = 
    {
        return data
    }

    //transform from json to parquet
    def writeParquet(path: String) : Unit = 
    {
        data.write.mode("append").parquet(path)
    }

    //print DataFrame Schema
    def printSchema() : Unit =
    {
        data.printSchema()
    }

    //show content in DataFrame
    def showContent() : Unit = 
    {
        data.show()
    }
    
}

