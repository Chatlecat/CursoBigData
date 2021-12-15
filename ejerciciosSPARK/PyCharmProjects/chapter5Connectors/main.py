# Example on use of connectors with pyspark (Chapter 5)
from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = (SparkSession.builder.appName("capitulo5UDFs").getOrCreate())

    # Loading data from a JDBC source using load
    jdbcDF = (spark
              .read
              .format("jdbc")
              .option("url", "jdbc:mysql://localhost:3306/ejemploschema?serverTimezone=UTC")
              .option("driver", "com.mysql.jdbc.Driver")
              .option("dbtable", "actor")
              .option("user", "root")
              .option("password", "[PASS]")
              .load())
    jdbcDF.show(truncate=False)

    #Si ocurre un fallo respecto de la zona horaria, hay que ejecutar la sentencia "SET GLOBAL time_zone=-0:00"
    #en la base de datos

    #Si hay que volver a hacer el procedimiento recordar meter el .jar del conector en la carpeta de
    #jars de spark
