package part2dataframes

import org.apache.spark.sql.SparkSession
import part2dataframes.Aggregations.spark

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .master("local")
    .getOrCreate()

  // Set log level to ERROR
  spark.sparkContext.setLogLevel("ERROR")

  println(spark.conf.getAll)



}
