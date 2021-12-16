#
from pyspark.sql import SparkSession, Window
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = (SparkSession.builder.appName("capitulo5_CommonFunctions").getOrCreate())

    tripdelaysFilePath = "file:///Users/carlos.borras/Desktop/departuredelays.csv"
    airportsnaFilePath = "file:///Users/carlos.borras/Desktop/airport-codes-na.txt"

    # Obtain airports data set
    airports = (spark.read
                .format("csv")
                .options(header="true", inferSchema="true", sep="\t")
                .load(airportsnaFilePath))
    airports.createOrReplaceTempView("airports_na")

    # Obtain departure delays data set
    delays = (spark.read
              .format("csv")
              .options(header="true")
              .load(tripdelaysFilePath))
    delays = (delays
              .withColumn("delay", expr("CAST(delay as INT) as delay"))
              .withColumn("distance", expr("CAST(distance as INT) as distance")))
    delays.createOrReplaceTempView("departureDelays")

    # Create temporary small table
    foo = (delays
           .filter(expr("""origin == 'SEA' and destination == 'SFO' and 
         date like '01010%' and delay > 0""")))
    foo.createOrReplaceTempView("foo")

    print("TABLA AEROPUERTOS\n")
    spark.sql("SELECT * FROM airports_na LIMIT 10").show()
    print("TABLA RETRASOS\n")
    spark.sql("SELECT * FROM departureDelays LIMIT 10").show()
    print("TABLA ORIGEN SEATTLE - DESTINO SAN FRANCISCO - EL 1 DEL 1 ANTES DE LAS 10 DE LA MAÑANA - CON RETRASO\n")
    spark.sql("SELECT * FROM foo").show()

    # UNIONS
    # Union two tables
    bar = delays.union(foo)
    bar.createOrReplaceTempView("bar")
    # Show the union (filtering for SEA and SFO in a specific time range)
    bar.filter(expr("""origin == 'SEA' AND destination == 'SFO'
    AND date LIKE '01010%' AND delay > 0""")).show()
    # COMO SE VE SE DUPLICAN LOS DATOS

    # JOINS
    foo.join(
        airports,
        airports.IATA == foo.origin
    ).select("City", "State", "date", "delay", "distance", "destination").show()

    # WINDOWING
    df_window1 = delays.groupBy("origin", "destination") \
        .agg(sum(col("delay")).alias("TotalDelays")) \
        .where(col("origin").isin("SEA", "SFO", "JFK") & col("destination").isin("SEA", "SFO", "JFK" \
                                                                                 , "DEN", "ORD", "LAX", "ATL"))
    print("PRIMERA FASE WINDOWING\n")
    df_window1.show(truncate=False)

    print("SEGUNDA FASE WINDOWING")
    window_spec = Window.partitionBy("origin").orderBy(desc("TotalDelays"))
    df_window2 = df_window1.select(col("origin"), col("destination"), col("TotalDelays") \
                                   , dense_rank().over(window_spec).alias("rank")) \
        .where(col("rank") <= 3)

    df_window2.show(truncate=False)

    #MODIFICATIONS
    #Adding new columns
    foo2 = (foo.withColumn(
        "status",
        expr("CASE WHEN delay <= 10 THEN 'On-time' ELSE 'Delayed' END")
    ))
    foo2.show(truncate=False)

    #Dropping columns
    foo3 = foo2.drop("delay")
    foo3.show()

    #Renaming columns
    foo4 = foo3.withColumnRenamed("status", "flight_status")
    foo4.show()

    #Pivoting
    #DF API
    delays.select(col("destination"), substring(col("date"), 0, 2).cast("Int").alias("month"), col("delay"))\
        .where(col("origin") == "SEA")\
        .show(10, truncate=False)

    delays.select(col("destination"), substring(col("date"), 0, 2).cast("Int").alias("month"), col("delay"))\
        .where(col("origin") == "SEA").groupBy(col("destination")).pivot("month")\
        .agg(avg(col("delay")), max(col("delay"))).orderBy(col("destination"))\
        .show(10, truncate=False)

    #SQL
    spark.sql("""SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
                     FROM departureDelays
                    WHERE origin = 'SEA'
                    LIMIT 10
                    """).show(truncate=False)

    spark.sql("""SELECT * FROM (
                    SELECT destination, CAST(SUBSTRING(date, 0, 2) AS int) AS month, delay
                     FROM departureDelays WHERE origin = 'SEA'
                    )
                    PIVOT (
                     CAST(AVG(delay) AS DECIMAL(4, 2)) AS AvgDelay, MAX(delay) AS MaxDelay
                     FOR month IN (1 JAN, 2 FEB)
                    )
                    ORDER BY destination"""\
              ).show(10, truncate=False)

