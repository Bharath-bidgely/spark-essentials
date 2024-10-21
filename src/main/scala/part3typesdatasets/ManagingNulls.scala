package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ManagingNulls extends App {
  val spark = SparkSession.builder()
    .appName("ComplexTypes")
    .master("local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  //moviesDF.show(truncate = false)
  //not null columns

  moviesDF.select(
    col("Title"),
    col("Rotten_Tomatoes_Rating"),
    col("IMDB_Rating"),
    coalesce(col("Rotten_Tomatoes_Rating"),col("IMDB_Rating")*10)
  )//.show(truncate = false)

  moviesDF.select(
    col("Title")
  ).where(col("Rotten_Tomatoes_Rating").isNull)
    //.show(2)

  moviesDF.select(
    col("Title")
  ).orderBy(col("IMDB_Rating").desc_nulls_last)

  //removing nulls
  moviesDF.select("Title","IMDB_Rating").na.drop()
    //.show(2)

  //replace nulls
  moviesDF.select("Title","IMDB_Rating").na.fill(0,List("IMDB_Rating")).show(2)
  moviesDF.na.fill (Map(
    "IMDB_Rating" -> 0,
    "Rotten_Tomatoes_Rating"   -> 10,
    "Director" -> "Unknow"
  )).show(2)


  //complex operations


}
