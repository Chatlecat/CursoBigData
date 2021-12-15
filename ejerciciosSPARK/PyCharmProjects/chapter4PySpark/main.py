# Ejercicios de PySpark del Chapter 4
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *


if __name__ == '__main__':
    spark = (SparkSession.builder.appName("capitulo4").getOrCreate())

    spark.sql("""CREATE OR REPLACE TEMPORARY VIEW delay_table
            USING parquet
            OPTIONS (
            path "file:///Users/carlos.borras/Desktop/parquet/2010-summary.parquet" )
            """)

    df = spark.sql("SELECT * FROM delay_table")

    (df.write.format("parquet")\
        .mode("overwrite")\
        .option("compression", "snappy")\
        .save("file:///Users/carlos.borras/Desktop/parquet/copia_2010_summit.parquet"))

    #PREGUNTAR COMO USAR AVRO CON PYSPARK

    #spark.sql("""CREATE OR REPLACE TEMPORARY VIEW episode_tbl
    #            USING avro
    #           OPTIONS (
    #                path "file:///Users/carlos.borras/Desktop/avro/*"
    #            )""")

    #df2 = spark.sql("SELECT * FROM episode_tbl")
    #df2.show(truncate=False)


