# Example on use of connectors with pyspark (Chapter 5) Ejercicio Employees
from pyspark.sql import SparkSession, Window
from pyspark.sql.types import *
from pyspark.sql.functions import *

if __name__ == '__main__':
    spark = (SparkSession.builder.appName("capitulo5EjercicioEmployees").getOrCreate())

    # i. Cargar con spark datos de empleados y departamentos

    # Loading data from a JDBC source using load
    employees_DF = (spark
                    .read
                    .format("jdbc")
                    .option("url", "jdbc:mysql://localhost:3306/employees?serverTimezone=UTC")
                    .option("driver", "com.mysql.jdbc.Driver")
                    .option("dbtable", "employees")
                    .option("user", "root")
                    .option("password", "[PASS]")
                    .load())

    department_DF = (spark
                     .read
                     .format("jdbc")
                     .option("url", "jdbc:mysql://localhost:3306/employees?serverTimezone=UTC")
                     .option("driver", "com.mysql.jdbc.Driver")
                     .option("dbtable", "departments")
                     .option("user", "root")
                     .option("password", "[PASS]")
                     .load())

    salaries_DF = (spark
                   .read
                   .format("jdbc")
                   .option("url", "jdbc:mysql://localhost:3306/employees?serverTimezone=UTC")
                   .option("driver", "com.mysql.jdbc.Driver")
                   .option("dbtable", "salaries")
                   .option("user", "root")
                   .option("password", "[PASS]")
                   .load())

    titles_DF = (spark
                 .read
                 .format("jdbc")
                 .option("url", "jdbc:mysql://localhost:3306/employees?serverTimezone=UTC")
                 .option("driver", "com.mysql.jdbc.Driver")
                 .option("dbtable", "titles")
                 .option("user", "root")
                 .option("password", "[PASS]")
                 .load())

    dept_emp_DF = (spark
                   .read
                   .format("jdbc")
                   .option("url", "jdbc:mysql://localhost:3306/employees?serverTimezone=UTC")
                   .option("driver", "com.mysql.jdbc.Driver")
                   .option("dbtable", "dept_emp")
                   .option("user", "root")
                   .option("password", "[PASS]")
                   .load())


    # ii. Mediante Joins mostrar toda la información de los empleados además
    # de su título y salario.

    employee_with_salary_and_title_DF = employees_DF.alias("emp"). \
        join(salaries_DF.alias("sal"), col("sal.emp_no") == col("emp.emp_no")). \
        join(titles_DF.alias("tit"), col("tit.emp_no") == col("emp.emp_no")). \
        join(dept_emp_DF.alias("demp"), col("emp.emp_no") == col("demp.emp_no")). \
        select(col("sal.emp_no"), col("first_name"), col("last_name"), col("salary"), col("title"), col("dept_no"),
               col("demp.from_date"))

    employee_with_salary_and_title_DF.show(truncate=False)

    # iii. Diferencia entre Rank y dense_rank (operaciones de ventana)
    #
    #   En el caso de haber valores iguales, Rank daría al siguiente valor más bajo
    #   el valor de nrank+numero de valores anteriore
    #   Si hay dos segundos en el ranking el siguiente sería el cuarto
    #
    #   Por el contrario dense_rank aún habiendo varios con el mismo rank
    #   el siguiente valor más bajo sería el siguiente
    #   Siguiendo el ejemplo anterior, dos segundos y el siguiente, tercero


    # iv. Utilizando operaciones de ventana obtener el salario, posición (cargo)
    # y departamento actual de cada empleado, es decir, el último o más
    # reciente.

    employee_with_dept_name_DF = employee_with_salary_and_title_DF.alias("ewd") \
        .join(department_DF.alias("dept"), col("dept.dept_no") == col("ewd.dept_no")) \
        .select(col("emp_no"), col("first_name"), col("last_name"), col("title"), col("salary"), col("dept_name"),
                col("from_date"))



    window_spec = Window.partitionBy("emp_no", "first_name", "last_name") \
        .orderBy(desc("from_date"))

    windowed = employee_with_dept_name_DF \
        .select(col("emp_no"), col("first_name"), col("last_name"), col("from_date"), col("salary"),
                col("title"), col("dept_name")) \
        .withColumn("rn", row_number().over(window_spec)).where(col("rn") == lit(1)).drop(col("rn"))\
        .orderBy(col("emp_no"))

    windowed.show(truncate=False)


