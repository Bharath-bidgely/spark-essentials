package part2dataframes

import Aggregations.spark
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._

object Aggregations extends App {

  //create spark session
  val spark = SparkSession.builder()
    .appName("Aggregations")
    .master("local")
    .config("spark.eventLog.enabled", "false")
    .config("spark.ui.showConsoleProgress", "false")
    .getOrCreate()

  // Set log level to ERROR
  spark.sparkContext.setLogLevel("ERROR")

  // Load movies DataFrame
  lazy val moviesDF: DataFrame = {
    val df = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
    //df.show(2) // Ensure the DataFrame is loaded
    df
  }

  if (moviesDF == null) {
    throw new NullPointerException("moviesDF is null")
  }

  // Method to get moviesDF
  def getMoviesDF: DataFrame = moviesDF

  //getMoviesDF.show(2)

  //counting records based on a column

  //select count(major_Genre) from moviesDF
  //val countMajorGenre2 = moviesDF.select(count(col("Major_Genre"))) //all values except nulls
  val countMajorGenre2 = moviesDF.selectExpr("count(Major_Genre) as Count")  //all values except nulls
  //countMajorGenre2.show()

  //select count(*) from moviesDF
  //moviesDF.selectExpr("count(*)") // count all rows including nulls

  //select count(distinct Major_Genre) from moviesDF;
  //moviesDF.select(count_distinct(col("Major_Genre")).as("Distinct_Count")).show
  //moviesDF.selectExpr("count(Distinct Major_Genre) AS Distinct_Count").show

  //moviesDF.select(approx_count_distinct(col("Major_Genre")).as("Approx_Dist_Count")).show()

  //min and max
  //select min(IMDB_Rating) from moviesDF
  val minRatingDF = moviesDF.select(min("IMDB_Rating"))//.show()

  //select sum(US_Gross) from moviesDF
  val sumRatingDF = moviesDF.select(sum("US_Gross"))//.show()

  //select avg(Rotten_Tomatoes_Rating) from moviesDF
  val avgRatingDF = moviesDF.select(avg("Rotten_Tomatoes_Rating"))//.show()

  //select mean(Rotten_Tomatoes_Rating),stddev(Rotten_Tomatoes_Rating) from moviesDF
  val dsRatingDF = moviesDF.select(
    mean("Rotten_Tomatoes_Rating"),
    stddev("Rotten_Tomatoes_Rating"))
  //.show()

  //Grouping

  //select Major_Genre,count(*) from moviesDF group by Major_Genre
  val groupbyGenreDF = moviesDF.groupBy(col("Major_Genre")).count().show() //includes nulls
  //val countMajorGenre = moviesDF.groupBy("Major_Genre").agg(count("*").as("count"))
  //countMajorGenre.show()

  val avgRatingbyGenreDF = moviesDF.groupBy(col("Major_Genre")).agg(
    avg("IMDB_Rating").as("Avg_IMDB_Rating"),
    count("*").as("Count")
  ).orderBy(col("Avg_IMDB_Rating"))
    .show()

  /**
   * Exercises
   * 1. sum
   * 2. count
   * 3. show
   * 4. Compute
   */


//  val sumOfTotalSalesDF = moviesDF.withColumn("Total_Sales",
//    col("US_Gross") + col("Worldwide_Gross") + coalesce(col("US_DVD_Sales"), lit(0))
//  )
//  sumOfTotalSalesDF.select("Title", "Total_Sales").show()

  /**
   * SELECT SUM(US_Gross + Worldwide_Gross + COALESCE(US_DVD_Sales, 0)) AS Total_Sales
   * FROM movies
   */
  val sumOfSales = moviesDF
    .select((col("US_Gross") + col("Worldwide_Gross") + coalesce(col("US_DVD_Sales"), lit(0))).as("Total_Sales"))
    .select(sum("Total_Sales")).show()

  val sumOfTotalSalesDF2 = moviesDF.groupBy("Title").agg((sum("US_Gross") + sum("Worldwide_Gross") + sum(coalesce(col("US_DVD_Sales"),lit(0)))).as("Total_Sales"))
  sumOfTotalSalesDF2.show()

  val distinctDirDF = moviesDF.select(count_distinct(coalesce(col("Director"), lit("No_Director"))).as("Distinct_Directors"))
  distinctDirDF.show()

  val distinctDirDF2 = moviesDF.select(count_distinct(coalesce(col("Director"))).as("Distinct_Directors"))
  distinctDirDF2.show()

  val usMeanAndStdDevDF = moviesDF.select(
    mean("US_Gross"),
    stddev("US_Gross"))
  .show()

  val dummyTestDF = moviesDF.groupBy("Director").agg(
    avg("IMDB_Rating").as("IMDB_Avg"),
    avg("US_Gross").as("US_AVG")
  ).orderBy(col("IMDB_Avg").desc_nulls_last)

  dummyTestDF.show()

}
