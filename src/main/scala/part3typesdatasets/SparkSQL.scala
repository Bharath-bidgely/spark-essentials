package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.col

object SparkSQL extends App {

  val spark = SparkSession.builder()
    .appName("SparkSQL")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .config("spark.sql.warehouse.dir", "spark-warehouse/databases")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val carsDF = spark.read
    .option("InferSchema", "true")
    .json("src/main/resources/data/cars.json")

  //carsDF.show(1,truncate = false)

  carsDF.select(col("Name")).where(col("Origin") === "USA")

  //use SparkSQl
  carsDF.createOrReplaceTempView("cars")
  val amerciansCars = spark.sql(
    """
      |select * from cars where origin = 'USA'
      |""".stripMargin)
  //amerciansCars.show(2)

  spark.sql("create database rtjvm")
  //we can write any SQL query we want.

  val databaseListDF = spark.sql("show databases")
  databaseListDF.show()

  //transfer DB tables to spark tables.


}
