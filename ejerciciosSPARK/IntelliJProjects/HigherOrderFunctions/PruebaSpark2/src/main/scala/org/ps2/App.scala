package org.ps2

import org.apache.spark.sql.{Column, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types.ArrayType

/**
 * @author ${user.name}
 *        Prueba de como usar Higher Order functions fuera de SparkSQL
 */
object App {



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
    tC.createOrReplaceTempView("tablaGrados")

    //FULL SQL
    spark.sql(
      """SELECT celsius, transform(celsius, x -> ((x*9) div 5)+32) as fahrenheit,
        |filter(celsius, x -> (x > 38)) as Higher_Celsius,
        |exists(celsius, x -> (x = 38)) as existe_38,
        |aggregate(celsius, 0, (t,acc) -> t + acc, acc -> (((acc div size(celsius) * 9 ) div 5) + 32 )) as avgFahrenheit
        |FROM tablaGrados
        |""".stripMargin).show(false)
    //SEMI SQL
    tC.select("celsius").
      withColumn("fahrenheit", expr("transform(celsius, x -> ((x*9)/5)+32)")). //transform()
      withColumn("Higher Celsius", expr("filter(celsius, x -> (x > 38))")).    //filter()
      withColumn("existe 38", expr("exists(celsius, x -> (x = 38))")).         //exists()
      withColumn("avgFahrenheit", expr("aggregate(celsius, 0, (t, acc) -> t+acc, acc -> (((acc/size(celsius) * 9 )/5) +32))")).
      show(false)

    //FULL
    tC.select("celsius").
      withColumn("fahrenheit", transform($"celsius", x => ((x*9)/5)+32)). //transform()
      withColumn("Higher Celsius", filter($"celsius", x => (x > 38))).    //filter()
      withColumn("existe 38", exists($"celsius", x => x === 38)).         //exists()
      withColumn("avgFahrenheit", aggregate($"celsius", lit(0), (t, acc) => t+acc, (acc) => (((acc/size($"celsius")) * lit(9)) / lit(5)) + lit(32) )).
      show(false)



  }

}
