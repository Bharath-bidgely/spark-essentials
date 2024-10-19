package part3typesdatasets

import org.apache.spark.sql.SparkSession

object ManagingNulls extends App {
  val spark = SparkSession.builder()
    .appName("ComplexTypes")
    .master("local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")




}
