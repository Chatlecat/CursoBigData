package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

/**
 * @author ${user.name}
 *        Prueba de como usar Higher Order functions fuera de SparkSQL
 */
object App {

  def main(args : Array[String]) {

    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("Funciones")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._

    val jdbcDF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/ejemploschema?serverTimezone=UTC")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "actor")
      .option("user", "root")
      .option("password", "[PASS]")
      .load()

    jdbcDF.show(false)
    // Saving data to a JDBC source using save
    /*jdbcDF
      .write
      .format("jdbc")
      .option("url", "jdbc:mysql://[DBSERVER]:3306/[DATABASE]")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "[TABLENAME]")
      .option("user", "[USERNAME]")
      .option("password", "[PASSWORD]")
      .save()
      */
  }
}
