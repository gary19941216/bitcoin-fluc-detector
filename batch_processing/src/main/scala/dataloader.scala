package dataload

import org.apache.spark.sql._

class DataLoader(val spark: SparkSession)
{
    private var data : Dataset[Row] = _  

    //load data from file
    def load(path: String) : Unit =
    {    
        data = spark.read.json(path)
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

    def printSchema() : Unit =
    {
        data.printSchema()
    }

    def showContent() : Unit = 
    {
        data.show()
    }

}


