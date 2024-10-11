package part2dataframes

import org.apache.spark.sql.SparkSession
import part2dataframes.Aggregations.spark
import org.apache.spark.sql.functions._
import part2dataframes.DataSources.spark

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .master("local")
    .getOrCreate()

  // Set log level to ERROR
  spark.sparkContext.setLogLevel("ERROR")

  // println(spark.conf.getAll)

  val guitarsDF = spark.read.option("infer_Schema","true").json("src/main/resources/data/guitars.json")
  //guitarsDF.show()

  val guitarPlayersDF = spark.read.option("infer_Schema","true").json("src/main/resources/data/guitarPlayers.json")
  //guitarPlayersDF.show()

  val bandsDF = spark.read.option("infer_Schema","true").json("src/main/resources/data/bands.json")
  //bandsDF.show()

  val joinCondition = guitarPlayersDF.col("band") === bandsDF.col("id")

  val guitarBandDF = guitarPlayersDF.join(bandsDF,joinCondition,"inner")
  guitarBandDF.show()
  //guitarBandDF.printSchema()

  //leftjoin
  //guitarPlayersDF.join(bandsDF,joinCondition,"left_outer").show()

  //rightjoin

  //fullouter
  //guitarPlayersDF.join(bandsDF,joinCondition,"outer").show()

  //leftsemi
  //guitarPlayersDF.join(bandsDF,joinCondition,"left_semi").show()

  //righsemi

  //antijoin - left_anti -- non equal joins
  //guitarPlayersDF.join(bandsDF,joinCondition,"left_anti").show()

  //guitarBandDF.select("id","band").show() //this should crash.
  //to handle the ambiguous columns option 1 is to rename

  val testDF = guitarPlayersDF.join(bandsDF.withColumnRenamed("id","band"),"band","inner")
  //testDF.printSchema()

  //Option-2 Drop the column
  guitarBandDF.drop(bandsDF.col("id"))

  //option 3 - rename and keep the data

  val bandsModDF = bandsDF.withColumnRenamed("id","bandid")

  if (guitarPlayersDF == null) {
    println("guitarPlayersDF is null")
  } else {
    println("guitarPlayersDF is not null")
  }

  if (bandsModDF == null) {
    println("bandsModDF is null")
  } else {
    println("bandsModDF is not null")
  }

  if (joinCondition2 == null) {
    println("joinCondition2 is null")
  } else {
    println("joinCondition2 is not null")
  }

  //rename the column and keep the data ( not a viable option )
  val joinCondition2 = guitarPlayersDF.col("band") === bandsModDF.col("bandid")
  guitarPlayersDF.join(bandsModDF,joinCondition2).show()

  //using complex types
  guitarPlayersDF.join(guitarsDF.withColumnRenamed("id","guitarid"), expr("array_contains(guitars,guitarid)")).show()

  /**
    * exercises
    *
    * max salary
    * never managers
    * job titles
    */

  def loadJdbcTable(spark: SparkSession, driver: String, url: String, user: String, password: String, dbtable: String) = {
    spark.read
      .format("jdbc")
      .option("driver", driver)
      .option("url", url)
      .option("user", user)
      .option("password", password)
      .option("dbtable", dbtable)
      .load()
  }

  val jdbcOptions = Map(
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
    "user" -> "docker",
    "password" -> "docker"
  )

  val employees_DF = loadJdbcTable(spark, jdbcOptions("driver"), jdbcOptions("url"), jdbcOptions("user"), jdbcOptions("password"), "public.employees")
  val salaries_DF = loadJdbcTable(spark, jdbcOptions("driver"), jdbcOptions("url"), jdbcOptions("user"), jdbcOptions("password"), "public.salaries")
  val dept_Manager_DF = loadJdbcTable(spark, jdbcOptions("driver"), jdbcOptions("url"), jdbcOptions("user"), jdbcOptions("password"), "public.dept_manager")

  val maxSalariesDF = salaries_DF.groupBy("emp_no").agg(max("salary"))
  val employees_Salaries_join = salaries_DF.col("emp_no") === employees_DF.col("emp_no")
  employees_DF.join(maxSalariesDF,employees_Salaries_join,"inner").drop(salaries_DF.col("emp_no")).show(2)

  val employees_Dept_Manager_Join = dept_Manager_DF.col("emp_no") === employees_DF.col("emp_no")
  employees_DF.join(dept_Manager_DF,employees_Dept_Manager_Join,"left_anti").drop(dept_Manager_DF.col("emp_no")).show(2)



  /**
   * rtjvm=# select * from employees limit 1;
   * emp_no | birth_date | first_name | last_name | gender | hire_date
   * --------+------------+------------+-----------+--------+------------
   * 10010 | 1963-06-01 | Duangkaew  | Piveteau  | F      | 1989-08-24
   * (1 row)
   *
   * rtjvm=# select * from salaries limit 1;
   * emp_no | salary | from_date  |  to_date
   * --------+--------+------------+------------
   * 10010 |  72488 | 1996-11-24 | 1997-11-24
   *
   * select max(salary) , emp_no from salaries group by emp_no
   * select employees.*, salaries.max_salary from public.employees join public.salaries on e.emp_no=s.emp_no;
   */



}
