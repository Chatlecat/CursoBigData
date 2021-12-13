# Prueba de como usar Higher Order functions fuera de SparkSQL
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

# Press the green button in the gutter to run the script.
if __name__ == '__main__':
    spark = (SparkSession.builder.appName("capitulo5").getOrCreate())

    # Higher-Order Functions
    schema = StructType([StructField("celsius", ArrayType(IntegerType()))])
    t_list = [[35, 36, 32, 30, 40, 42, 38]], [[31, 32, 34, 55, 56]]
    t_c = spark.createDataFrame(t_list, schema)

    df_celsius_and_fahr = t_c.select('celsius') \
        .withColumn('fahrenheit', transform('celsius', lambda x: (x * 9) / 5 + 32)) \
        .withColumn('Higher Celsius', filter('celsius', lambda x: x > 38)) \
        .withColumn('existe 38', exists('celsius', lambda x: x == 38)) \
        .withColumn('avgFahrenheit',
                    aggregate('celsius', lit(0), lambda x, y: x + y, lambda y: (((y / size('celsius')) * 9) / 5) + 32))
    # transform()
    # filter()
    # exists()

    df_celsius_and_fahr.show(truncate=False)
