package dataload

import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.DataFrame

class DataLoader(val spark: SparkSession, val schema : StructType)
{
    private var data : DataFrame = _

    //load csv data from file
    def loadCsv(path: String) : DataLoader = 
    {   
        //csv file with no header
        data = spark.read.schema(schema).csv(path)
        return this
    }

    //load json data from file
    def loadJson(path: String) : DataLoader =
    {    
        data = spark.read.schema(schema).json(path)
        return this
    }

    //load avro data from file
    def loadAvro(path: String) : DataLoader = 
    {
        data = spark.read.format("avro").load(path)
        return this
    }

    def loadParquet(path: String) : DataLoader = 
    {
        data = spark.read.schema(schema).parquet(path)
        return this
    }

    //retrieve data
    def getData() : DataFrame = 
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
    def show(num: Int) : Unit = 
    {
        data.show(num)
    } 

    //take only the specify numbers of row
    def take(num: Int) : Array[Row] = 
    {
        return data.take(num)
    }
    
}

