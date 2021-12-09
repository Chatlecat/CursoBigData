#!/usr/bin/env python
# coding: utf-8

# ## EJERCICIOS PROPUESTOS POR JOSE ANTONIO
# 
#  3. Capítulo 4
#      a. Realizar todos los ejercicios propuestos de libro
#      b. GlobalTempView vs TempView
#      c. Leer los AVRO, Parquet, JSON y CSV escritos en el cap3
# 

# In[1]:


import findspark
findspark.init()


# In[2]:


from pyspark.sql import SparkSession
from pyspark.sql.types import *
from pyspark.sql.functions import *

spark = SparkSession.builder.appName("Chapter-4").getOrCreate()


# In[7]:


file = "C://Users/carlos.borras/Desktop/json/2010-summary.json"
file1 = "C://Users/carlos.borras/Desktop/json/2011-summary.json"
file2 = "C://Users/carlos.borras/Desktop/json/2012-summary.json"
file3 = "C://Users/carlos.borras/Desktop/json/2013-summary.json"
file4 = "C://Users/carlos.borras/Desktop/json/2014-summary.json"
files = [file1, file2, file3, file4]

#Otra manera es crear el DataFrame vacío con un schema
#df_flights = spark.createDataFrame([], schema)
df_flights = spark.read.format("json").option("inferSchema", "true").load(file)

for f in files:
    df_flights = df_flights.union(spark.read.format("json").option("inferSchema", "true").load(f)) 
    
print(df_flights.count())


# # b. GlobalTempView VS. TempView
# Las GlobalTempoView pueden ser usadas a través de distintas SparkSession mientras que las TempView normales sólo pueden ser usadas por la SparkSession que las creó. Esto puede ser útil para combinar datos de distintas metastores.
# 

# In[6]:


# c. Leer los AVRO, Parquet, JSON y CSV escritos en el cap3
# Parquet
file = "C:/Users/carlos.borras/Desktop/parquet/2010-summary.parquet/"

df_flights = spark.read.format("parquet").load(file)
    
print(df_flights.count())


# In[ ]:


# c. Leer los AVRO, Parquet, JSON y CSV escritos en el cap3
# CSV
file = "C://Users/carlos.borras/Desktop/csv/2010-summary.csv"

file1 = "C://Users/carlos.borras/Desktop/csv/2011-summary.csv"
file2 = "C://Users/carlos.borras/Desktop/csv/2012-summary.csv"
file3 = "C://Users/carlos.borras/Desktop/csv/2013-summary.csv"
file4 = "C://Users/carlos.borras/Desktop/csv/2014-summary.csv"
files = [file1, file2, file3, file4]

#Otra manera es crear el DataFrame vacío con un schema
#df_flights = spark.createDataFrame([], schema)
df_flights = spark.read.format("csv").option("inferSchema", "true").load(file)

for f in files:
    df_flights = df_flights.union(spark.read.format("csv").option("inferSchema", "true").load(f)) 
    
print(df_flights.count())


# In[ ]:


# c. Leer los AVRO, Parquet, JSON y CSV escritos en el cap3
# AVRO
file = "C://Users/carlos.borras/Desktop/avro/_/*"

df_flights = (spark.read.format("avro")            .option("inferSchema", "true").load(file))

#for f in files:
#    df_flights = df_flights.union(spark.read.format("csv").option("inferSchema", "true").load(f)) 
    
print(df_flights.count())


# In[ ]:




