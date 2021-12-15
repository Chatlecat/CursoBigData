package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * @author ${user.name}
 */

object App {

  def main(args : Array[String]) {

    val spark:SparkSession = SparkSession.builder().master("local[1]")
      .appName("Funciones")
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    val csvPath = "file:///Users/carlos.borras/Desktop/departuredelays.csv"

    val df_departure_delays = spark.read.format("csv").
      option("inferSchema", "true").
      option("header", "true").
      load(csvPath)

    df_departure_delays.createOrReplaceTempView("departure_delays_table")

    //Example of Spark DF query
    df_departure_delays.withColumn("Experienced_Delay", when(col("delay") > 360, "Very Long Delay").
                                                                  when(col("delay") <= 360 && col("delay") > 120, "Long delay").
                                                                  when(col("delay") <= 120 && col("delay") > 60, "Short Delay").
                                                                  when(col("delay") <= 60 && col("delay") > 0, "Tolerable Delay").
                                                                  otherwise("No Delay")).
                                                                  orderBy(desc("delay")).
                                                                  show(false)

    //Example of Spark SQL query
    spark.sql("""SELECT delay, origin, destination,
                             CASE
                               WHEN delay > 360 THEN 'Very Long Delays'
                               WHEN delay > 120 AND delay < 360 THEN 'Long Delays'
                               WHEN delay > 60 AND delay < 120 THEN 'Short Delays'
                               WHEN delay > 0 and delay < 60 THEN 'Tolerable Delays'
                               WHEN delay = 0 THEN 'No Delays'
                               ELSE 'Early'
                             END AS Flight_Delays
                           FROM departure_delays_table
                           ORDER BY delay DESC""")


    //Create Database and USE it
    spark.sql("CREATE DATABASE learn_spark_db")
    spark.sql("USE learn_spark_db")

    //Create Table using data
    spark.sql("""CREATE TABLE departure_delays_table2(date STRING, delay INT,
                         distance INT, origin STRING, destination STRING)
                         USING csv OPTIONS (PATH
                         'file:///Users/carlos.borras/Desktop/departuredelays.csv')""")

    spark.sql("SELECT * FROM departure_delays_table2").show()

    //Create table using DF API
    //(df_departure_delays
    // .write
    // .option("path", "/tmp/data/us_flights_delay")
    // .saveAsTable("us_delay_flights_tbl")

    //Save a table, query as DF
    val fake_df = spark.sql("SELECT * FROM departure_delays_table WHERE origin LIKE 'SFO'")
    fake_df.show(false)

    //Save as file (csv, parquet and avro)
    var location_to_write = "file:///Users/carlos.borras/Desktop/csv2"
    fake_df.write.mode("overwrite").format("csv").option("header", "true").save(location_to_write)
    location_to_write = "file:///Users/carlos.borras/Desktop/parquet2"
    fake_df.write.mode("overwrite").format("parquet").option("header", "true").save(location_to_write)
    location_to_write = "file:///Users/carlos.borras/Desktop/avro2"
    fake_df.write.mode("overwrite").format("avro").option("header", "true").save(location_to_write)



  }

}
