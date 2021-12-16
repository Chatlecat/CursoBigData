package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window


/**
 * @author ${user.name}
 *        Common Functions for DF API and SQL (Learning Spark 2nd Edition)
 */
object App {

  def main(args : Array[String]) {

    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("FuncionesComunes")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._


    // Set file paths
    val delaysPath =
      "file:///Users/carlos.borras/Desktop/departuredelays.csv"
    val airportsPath =
      "file:///Users/carlos.borras/Desktop/airport-codes-na.txt"
    // Obtain airports data set
    val airports = spark.read
      .option("header", "true")
      .option("inferschema", "true")
      .option("delimiter", "\t")
      .csv(airportsPath)
    airports.createOrReplaceTempView("airports_na")
    // Obtain departure Delays data set
    val delays = spark.read
      .option("header","true")
      .csv(delaysPath)
      .withColumn("delay", expr("CAST(delay as INT) as delay"))
      .withColumn("distance", expr("CAST(distance as INT) as distance"))
    delays.createOrReplaceTempView("departureDelays")

    // Create temporary small table
    val foo = delays.filter(
      expr("""origin == 'SEA' AND destination == 'SFO' AND
                     date like '01010%' AND delay > 0"""))
    foo.createOrReplaceTempView("foo")

    println("TABLA AEROPUERTOS")
    spark.sql("SELECT * FROM airports_na LIMIT 10").show()
    println("TABLA RETRASOS")
    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()
    println("TABLA ORIGEN SEATTLE - DESTINO SAN FRANCISCO - EL 1 DEL 1 ANTES DE LAS 10 DE LA MAÑANA - CON RETRASO")
    spark.sql("SELECT * FROM foo").show()


    //UNIONS

    // Union two tables
    val bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")
    bar.filter(expr("""origin == 'SEA' AND destination == 'SFO'
                      AND date LIKE '01010%' AND delay > 0""")).show()

    //COMO SE VE SE DUPLICAN LOS DATOS EN LA EJECUCION

    //JOINS
    foo.join(
      airports.as('air),
      $"air.IATA" === $"origin"
    ).select("City", "State", "date", "delay", "distance", "destination").show()

    //Windowing (Hive support needed)
    //spark.sql("""CREATE TABLE departureDelaysWindow AS
    //            |SELECT origin, destination, SUM(delay) AS TotalDelays
    //            | FROM departureDelays
    //            |WHERE origin IN ('SEA', 'SFO', 'JFK')
    //            | AND destination IN ('SEA', 'SFO', 'JFK', 'DEN', 'ORD', 'LAX', 'ATL')
    //            |GROUP BY origin, destination;""".stripMargin)
    //spark.sql("SELECT * FROM departureDelaysWindow").show()

    //val df_windowing_ = Window.partitionBy("origin","destination")
    val df_window1 = delays.groupBy("origin", "destination")
      .agg(sum(col("delay")).as("TotalDelays"))
      .where(col("origin").isin("SEA", "SFO", "JFK") && col("destination").isin("SEA", "SFO", "JFK"
      , "DEN", "ORD", "LAX", "ATL"))
    println("PRIMERA FASE WINDOWING")
    df_window1.show(false)

    println("SEGUNDA FASE WINDOWING")
    val window_spec = Window.partitionBy("origin").orderBy(desc("TotalDelays"))
    val df_window2 = df_window1.select(col("origin"), col("destination"), col("TotalDelays")
      , dense_rank().over(window_spec).as("rank"))
      .where(col("rank") <= 3)

    df_window2.show(false)



    //MODIFICATIONS
    //Add new Columns
    val foo2 = foo.withColumn(
      "status",
      expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
    )
    foo2.show(false)

    //Drop Columns
    val foo3 = foo2.drop("delay")
    foo3.show()

    //Rename Columns
    val foo4 = foo3.withColumnRenamed("status", "flight_status")
    foo4.show()

    //Pivoting
    //DF Form
    delays.select(col("destination"), substring(col("date"), 0, 2).cast("Int").as("month"), col("delay"))
      .where(col("origin") === "SEA")
      .show(10, false)

    delays.select(col("destination"), substring(col("date"), 0, 2).cast("Int").as("month"), col("delay"))
      .where(col("origin") === "SEA").groupBy(col("destination")).pivot("month")
      .agg(avg(col("delay")), max(col("delay"))).orderBy(col("destination"))

    //SQL Form
    spark.sql("""SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
                | FROM departureDelays
                |WHERE origin = 'SEA'
                |LIMIT 10
                |""".stripMargin).show(false)

    spark.sql("""SELECT * FROM (
                |SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
                | FROM departureDelays WHERE origin = 'SEA'
                |)
                |PIVOT (
                | CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay, MAX(delay) AS MaxDelay
                | FOR month IN (1 JAN, 2 FEB)
                |)
                |ORDER BY destination"""
              .stripMargin).show(10, false)



  }
}
