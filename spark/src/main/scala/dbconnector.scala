package dbconnector

import org.apache.spark.sql._
import org.apache.spark.sql.types.{StructField, StructType}
import org.apache.spark.sql.DataFrame

// dbconnector to connect to cassandra
class DBConnector(val spark: SparkSession)
{
    // read data from cassandra
    def readFromCassandra(table: String, keyspace: String): DataFrame =
    {
        spark.read
	.format("org.apache.spark.sql.cassandra")
	.options(Map("table" -> table, "keyspace" -> keyspace))
	.load
    }

    // write data to cassandra
    def writeToCassandra(data: DataFrame, table: String, keyspace: String): Unit =
    {
        data.write
        .format("org.apache.spark.sql.cassandra")
        .mode("append")
        .options(Map("table" -> table, "keyspace" -> keyspace))  
        .save()
    }

    // overwrite data in cassandra
    def overwriteCassandra(data: DataFrame, table: String, keyspace: String): Unit =
    {
        data.write
        .format("org.apache.spark.sql.cassandra")
        .mode("overwrite").option("confirm.truncate","true")
        .options(Map("table" -> table, "keyspace" -> keyspace))
        .save()
    }

}
