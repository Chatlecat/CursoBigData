package org.example

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._

/**
 * @author ${user.name}
 *        Ejercicio Employees Capítulo 5
 */
object App {

  def main(args : Array[String]) {


    val spark: SparkSession = SparkSession.builder().master("local[1]")
      .appName("Capitulo_5_Ejercicio_Employees")
      .getOrCreate()
    spark.sparkContext.setLogLevel("ERROR")
    import spark.implicits._
    // i. Cargar con spark datos de empleados y departamentos
    //Getting data from Employees
    val employees_DF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees?serverTimezone=UTC")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "employees")
      .option("user", "root")
      .option("password", "[PASS]")
      .load()

    val department_DF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees?serverTimezone=UTC")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "departments")
      .option("user", "root")
      .option("password", "[PASS]")
      .load()

    val salaries_DF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees?serverTimezone=UTC")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "salaries")
      .option("user", "root")
      .option("password", "[PASS]")
      .load()

    val titles_DF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees?serverTimezone=UTC")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "titles")
      .option("user", "root")
      .option("password", "[PASS]")
      .load()

    val dept_emp_DF = spark
      .read
      .format("jdbc")
      .option("url", "jdbc:mysql://localhost:3306/employees?serverTimezone=UTC")
      .option("driver", "com.mysql.jdbc.Driver")
      .option("dbtable", "dept_emp")
      .option("user", "root")
      .option("password", "[PASS]")
      .load()



    // ii. Mediante Joins mostrar toda la información de los empleados además
    // de su título y salario.
    val employee_with_salarie_and_title_DF = employees_DF.as('emp).join(salaries_DF.as('sal), $"sal.emp_no" === $"emp.emp_no")
      .join(titles_DF.as('tit), $"tit.emp_no" === $"emp.emp_no")
      .select("emp.emp_no", "first_name", "last_name", "salary", "tit.from_date", "tit.to_date", "title")

    employee_with_salarie_and_title_DF.show(false)


    // iii. Diferencia entre Rank y dense_rank (operaciones de ventana)
    //
    //  En el caso de haber valores iguales, Rank daría al siguiente valor más bajo
    //  el valor de nrank+numero de valores anteriore
    //  Si hay dos segundos en el ranking el siguiente sería el cuarto
    //
    //  Por el contrario dense_rank aún habiendo varios con el mismo rank
    //  el siguiente valor más bajo sería el siguiente
    //  Siguiendo el ejemplo anterior, dos segundos y el siguiente, tercero


    // iv. Utilizando operaciones de ventana obtener el salario, posición (cargo)
    // y departamento actual de cada empleado, es decir, el último o más
    // reciente.

    //Preparamos los datos
    val empoyees_with_dept = employee_with_salarie_and_title_DF.as('emp)
      .join(dept_emp_DF.as('dep_emp), $"dep_emp.emp_no"===$"emp.emp_no")
      .select(
        col("emp.emp_no").as("emp_no")
        , col("first_name"), col("last_name"), col("title")
        , col("salary"), col("dep_emp.dept_no").as("dept_no"), col("emp.from_date")
        , col("emp.to_date")
      )
    val employees_dept_name = empoyees_with_dept.as('ewd).join(department_DF.as('dep), $"ewd.dept_no" === $"dep.dept_no")
      .select(col("emp_no"), col("first_name"), col("last_name"), col("title")
        , col("dep.dept_no").as("dept_no")
        , col("dep.dept_name").as("dept_name"), col("from_date")
        , col("to_date"), col("salary"))

    //Declaramos el window spec
    val window_spec = Window.partitionBy(  "emp_no", "first_name", "last_name")
      .orderBy(desc("from_date"))

    //Filtramos los datos para que nos de sólo el más reciente
    val windowed = employees_dept_name.select(col("emp_no")
      , col("first_name")
      , col("last_name")
      , col("from_date")
      , col("to_date")
      , col("salary")
      , col("title")
      , col("dept_name"))
        .withColumn("rn", row_number().over(window_spec))
        .where(col("rn")===lit(1))
        .drop(col("rn"))

    windowed.show(false)



  }
}
