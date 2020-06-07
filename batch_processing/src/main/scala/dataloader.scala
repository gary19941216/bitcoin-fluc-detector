package dataload

import org.apache.spark.sql._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.types.{StructField, StructType}

class DataLoader(val spark: SparkSession)
{
    private var data : Dataset[Row] = _
    private var schema : StructType = _

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
    def show(num: Int) : Unit = 
    {
        data.show(num)
    }
    
    //load the schema
    def loadSchema(schema: StructType) : Unit = 
    {
        this.schema = schema
    }

    //take only the specify numbers of row
    def take(num: Int) : Array[Row] = 
    {
        return data.take(num)
    }
    
}

