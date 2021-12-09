#Ejercicios en PySpark

#CADA SEPARACIÓN DE "#" MARCA UNA CELDA DEL NOTEBOOK

#ESTE ARCHIVO ES PARA FACILITAR LA LEGIBILIDAD EN GitHub DEL NOTEBOOK DE EJERCICIOS CON PYSPARK

#############################################################################################
#2. Capítulo 3

#   a. Realizar todos los ejercicios propuestos de libro

#   b. Leer el CSV del ejemplo del cap2 y obtener la estructura del schema dado por 
#   defecto.

#   c. Cuando se define un schema al definir un campo por ejemplo StructField('Delay', 
#   FloatType(), True) ¿qué significa el último parámetro Boolean?

#   d. Dataset vs DataFrame (Scala). ¿En qué se diferencian a nivel de código?

#   e. Utilizando el mismo ejemplo utilizado en el capítulo para guardar en parquet y 
#   guardar los datos en los formatos:

#      i.   JSON
#      ii.  CSV (dándole otro nombre para evitar sobrescribir el fichero origen)
#      iii. AVRO

#   f. Revisar al guardar los ficheros (p.e. json, csv, etc) el número de ficheros 
#   creados, revisar su contenido para comprender (constatar) como se guardan.

#      i.   ¿A qué se debe que hayan más de un fichero?

#      ii.  ¿Cómo obtener el número de particiones de un DataFrame?

#      iii. ¿Qué formas existen para modificar el número de particiones de un 
#      DataFrame?

#      iv.  Llevar a cabo el ejemplo modificando el número de particiones a 1 y 
#      revisar de nuevo el/los ficheros guardados.

##########################################################################################
import findspark
findspark.init()

#############################################################################################

from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Chapter-3").getOrCreate()
    

# Get the path to the CSV file
csvFile = "C://Users//carlos.borras//Downloads//sf-fire-calls.csv"


fire_schema = StructType([StructField('CallNumber', IntegerType(), True),
StructField('UnitID', StringType(), True),
StructField('IncidentNumber', IntegerType(), True),
StructField('CallType', StringType(), True),
StructField('CallDate', StringType(), True),
StructField('WatchDate', StringType(), True),
StructField('CallFinalDisposition', StringType(), True),
StructField('AvailableDtTm', StringType(), True),
StructField('Address', StringType(), True),
StructField('City', StringType(), True),
StructField('Zipcode', IntegerType(), True),
StructField('Battalion', StringType(), True),
StructField('StationArea', StringType(), True),
StructField('Box', StringType(), True),
StructField('OriginalPriority', StringType(), True),
StructField('Priority', StringType(), True),
StructField('FinalPriority', IntegerType(), True),
StructField('ALSUnit', BooleanType(), True),
StructField('CallTypeGroup', StringType(), True),
StructField('NumAlarms', IntegerType(), True),
StructField('UnitType', StringType(), True),
StructField('UnitSequenceInCallDispatch', IntegerType(), True),
StructField('FirePreventionDistrict', StringType(), True),
StructField('SupervisorDistrict', StringType(), True),
StructField('Neighborhood', StringType(), True),
StructField('Location', StringType(), True),
StructField('RowID', StringType(), True),
StructField('Delay', FloatType(), True)])


df_firecalls = spark.read.csv(csvFile, header=True, schema=fire_schema)
# OTRA FORMA: schema = "`Id` INT,`First` STRING,`Last` STRING,`Url` STRING,`Published` STRING,`Hits` INT,`Campaigns` ARRAY<STRING>"
#OJO CUIDAO LAS COMILLAS

#############################################################################################

df_firecalls.cache() #Guardamos en memoria para accesos rápidos y poco costosos

#############################################################################################

df_firecalls.count()

#############################################################################################

df_firecalls.printSchema()

#############################################################################################

# - What were all the different types of fire calls in 2018? - SQL

df_fechas_tipo = df_firecalls.select(to_date(col("CallDate"), "MM/dd/yyyy").alias("date"), col("CallType"))
df_fechas_tipo.createOrReplaceTempView("tabla_fire_calls")

df_diffCalls = spark.sql("SELECT DISTINCT(CallType) AS `Tipo Llamada` FROM tabla_fire_calls WHERE YEAR(date) LIKE 2008")
df_diffCalls.show(truncate=False)   

#############################################################################################

# - What were all the different types of fire calls in 2018? - SPARK SQL

df_distinct_calls = df_fechas_tipo.filter(year(col("date")) == 2008).select((col("CallType"))).distinct()
df_distinct_calls.show(df_distinct_calls.count(), truncate=False)

#############################################################################################

# - What months within the year 2018 saw the highest number of fire calls? - SQL

df_months_with_most_calls_sql = spark.sql("SELECT MONTH(date) AS Month, COUNT(*) AS count FROM tabla_fire_calls WHERE YEAR(date) LIKE 2018 GROUP BY Month ORDER BY COUNT(*) DESC")
df_months_with_most_calls_sql.show()

#############################################################################################

# - What months within the year 2018 saw the highest number of fire calls? - PYSPARK SQL

df_months_with_most_calls_pyspark = df_fechas_tipo.filter(year(col("date")) == 2018).select(month(col("date")).alias("Mes")).groupBy("Mes").count().orderBy(desc("count"))
df_months_with_most_calls_pyspark.show()

#############################################################################################

# - Which neighborhood in San Francisco generated the most fire calls in 2018? - SQL 

callDate_neighborhoods_delay = df_firecalls.select(to_date(col("CallDate"), "MM/dd/yyyy").alias("date"), col("Neighborhood"), col("Delay"))
callDate_neighborhoods_delay.createOrReplaceTempView("callDate_neighborhoods_delay")
df_neighborhood_most_calls = spark.sql("SELECT Neighborhood, COUNT(*) AS Num_Llamadas FROM callDate_neighborhoods_delay WHERE YEAR(date) LIKE 2018 GROUP BY Neighborhood ORDER BY Num_Llamadas DESC")
df_neighborhood_most_calls.show(3, truncate=False)

#############################################################################################

# - Which neighborhood in San Francisco generated the most fire calls in 2018? - PYSPARK SQL (MEJOR GESTION DE ERRORES, MEJOR MANTENIMIENTO)

df_neighborhood_most_calls_pyspark = callDate_neighborhoods_delay.filter(year(col("date"))==2018)\
                                                                .select(col("Neighborhood").alias("Barrio"))\
                                                                .groupBy("Barrio")\
                                                                .count().withColumnRenamed("count","num_llamadas")\
                                                                .orderBy(desc("num_llamadas"))
df_neighborhood_most_calls_pyspark.show(3, truncate=False)

#############################################################################################

# - Which neighborhoods had the worst response times to fire calls in 2018? - PYSPARK SQL

df_neighborhoods_worst_delay = callDate_neighborhoods_delay.filter(year(col("date"))==2018).\
                                                            select(col("Neighborhood").alias("Barrio"), col("Delay")).\
                                                            groupBy("Barrio").\
                                                            agg(avg("Delay").alias("Retraso_promedio")).\
                                                            orderBy(desc("Retraso_promedio"))
df_neighborhoods_worst_delay.show(3, truncate=False)      


#############################################################################################

# - Which neighborhoods had the worst response times to fire calls in 2018? - PYSPARK SQL

df_neighborhoods_worst_delay = callDate_neighborhoods_delay.filter(year(col("date"))==2018).\
                                                            select(col("Neighborhood").alias("Barrio"), col("Delay")).\
                                                            groupBy("Barrio").\
                                                            agg(avg("Delay").alias("Retraso_promedio")).\
                                                            orderBy(desc("Retraso_promedio"))
df_neighborhoods_worst_delay.show(3, truncate=False)      

#############################################################################################

# - Which week in the year in 2018 had the most fire calls? - SQL

df_week_most_calls_sql = spark.sql("SELECT weekofyear(date) AS Semana, COUNT(*) AS Num_llamadas\
                                FROM callDate_neighborhoods_delay\
                                WHERE YEAR(date) LIKE 2018\
                                GROUP BY Semana\
                                ORDER BY Num_llamadas DESC")

df_week_most_calls_sql.show(3, truncate = False)

#############################################################################################

# - Which week in the year in 2018 had the most fire calls? - PYSPARK SQL

df_week_most_calls = callDate_neighborhoods_delay.filter(year(col("date"))==2018).\
                                                    select(weekofyear(col("date")).alias("Semana")).\
                                                    groupBy("Semana").\
                                                    count().withColumnRenamed("count", "num_llamadas").\
                                                    orderBy(desc("num_llamadas"))
    

df_week_most_calls.show(3, truncate = False)

#############################################################################################
