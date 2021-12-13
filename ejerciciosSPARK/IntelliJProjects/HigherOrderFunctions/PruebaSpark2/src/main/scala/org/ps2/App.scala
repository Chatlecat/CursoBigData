package org.ps2

import org.apache.spark.sql.SparkSession

import org.apache.spark.sql.functions._

/**
 * @author ${user.name}
 *        Prueba de como usar Higher Order functions fuera de SparkSQL
 */
object App {
  
  def foo(x : Array[String]) = x.foldLeft("")((a,b) => a + b)
  
  def main(args : Array[String]) {

    val spark:SparkSession = SparkSession.builder().master("local[1]")
      .appName("Funciones")
      .getOrCreate()
    import spark.implicits._

    spark.sparkContext.setLogLevel("ERROR")

    //Higher-Order Functions Example Celsius to Fahrenheit
    val t1 = Array(35, 36, 32, 30, 40, 42, 38)
    val t2 = Array(31, 32, 34, 55, 56)
    val tC = Seq(t1, t2).toDF("celsius")


    tC.select("celsius").
      withColumn("fahrenheit", expr("transform(celsius, x -> ((x*9)/5)+32)")). //transform()
      withColumn("Higher Celsius", expr("filter(celsius, x -> (x > 38))")).    //filter()
      withColumn("existe 38", expr("exists(celsius, x -> (x = 38))")).         //exists()
      withColumn("avgFahrenheit", expr("aggregate(celsius, 0, (t, acc) -> t+acc, acc -> (((acc/size(celsius) * 9 )/5) +32))")).
      show(false)


  }

}
