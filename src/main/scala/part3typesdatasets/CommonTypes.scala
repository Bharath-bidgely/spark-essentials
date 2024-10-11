package part3typesdatasets

import org.apache.spark.sql.SparkSession
import part2dataframes.Aggregations
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import part2dataframes.Aggregations.spark

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("CommonTypes")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val moviesdf = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  //moviesdf.printSchema()



}