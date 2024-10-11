package part2dataframes

import org.apache.spark.sql.{DataFrame, SparkSession}
import part2dataframes.Aggregations.spark

object DataFrameCreator {
  println("Initializing DataFrameCreator object")

  lazy val spark: SparkSession = {
    println("Creating SparkSession")
    SparkSession.builder()
      .appName("DataFrame Creator")
      .master("local[*]")
      .getOrCreate()
  }

  lazy val moviesDF: DataFrame = {
    println("Creating moviesDF")
    try {
      val data = Seq(
        (1, "Movie A", 2020),
        (2, "Movie B", 2021),
        (3, "Movie C", 2022)
      )
      val columns = Seq("id", "title", "year")
      spark.createDataFrame(data).toDF(columns: _*)
    } catch {
      case e: Exception =>
        println(s"Error creating moviesDF: ${e.getMessage}")
        null
    }
  }

  def getMoviesDF: DataFrame = {
    println("getMoviesDF called")
    if (moviesDF == null) {
      println("moviesDF is null")
    }
    moviesDF
  }

  // Load movies DataFrame
  lazy val movies1DF: DataFrame = {
    val df = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
    //df.show(2) // Ensure the DataFrame is loaded
    df
  }

  lazy val df1 = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")

}