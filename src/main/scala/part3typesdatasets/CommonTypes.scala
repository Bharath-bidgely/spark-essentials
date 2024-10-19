package part3typesdatasets

import org.apache.spark.sql.SparkSession
import part2dataframes.Aggregations
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.functions._
import part2dataframes.Aggregations.{moviesDF, spark}

object CommonTypes extends App {

  val spark = SparkSession.builder()
    .appName("CommonTypes")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val moviesDf = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  //moviesdf.printSchema()

  //Add a plain value to the dataframe
  moviesDf.select(col("Title"), lit(47).as("plan_value")).show(2)

  //booleans
  //"Major_Genre":"Drama"
  //"IMDB_Rating" > 7
  // select when both of these filters are true

  val dramaFilter = col("Major_Genre") equalTo "Drama"
  val goodRatingFilter = col("IMDB_Rating") > 7.0
  val preferredMovies = dramaFilter and goodRatingFilter

  println("dramafilter")
//  moviesDf.select("Title", "Major_Genre").where(col("Major_Genre").equalTo("Drama")).show(2)
  //moviesDf.select("Title").filter(preferredMovies).show(2)
  val testDf = moviesDf.select(col("Title"), preferredMovies.as("Preferred_movies"))
  //testDf.filter("Preferred_movies").show(2)
  //testDf.filter(not(col("Preferred_movies"))).show(2)

  //numbers
  val moviesavgratingDF = moviesDf.select(col("Title"), ((coalesce(col("Rotten_Tomatoes_Rating"), lit(0))/10 + coalesce(col("IMDB_Rating"), lit(0)))/2).as("avg_rating"))
  //moviesavgratingDF.show(2)

  //correlation
  //println(moviesDf.stat.corr("Rotten_Tomatoes_Rating", "IMDB_Rating"))

  //Strings
//  moviesDf.select(initcap(col("Distributor"))).show(2)
//  moviesDf.select("*").where(col("Title").contains("The")).show(2)

  //regex
  val regEx="The|A"
  moviesDf.select(
    col("*") ,
    regexp_extract(col("Title"),regEx,0).as("reg_ex")
  ).filter(col("reg_ex") =!= "").drop("reg_ex")
    //.show(2)

  /**
   * Filter the cars DF by list of car names obtained by an API call
   */
  val MovieNameList = List("To Kill A Mockingbird","Tora, Tora, Tora")
  def filterMovieName(movienames: List[String]) :DataFrame  = {
    moviesDf.filter(col("Title").isin(MovieNameList: _*)) //_* means passing the list as varry inputs one by one in sequence
  }

  val filteredMovieNameList = filterMovieName(MovieNameList)
  filteredMovieNameList.show(2)

  // Other way of writing this.
  val complexNames = MovieNameList.map(_.toLowerCase()).mkString("|")
  moviesDf.select(
    col("*"),
    regexp_extract(lower(col("Title")),complexNames,0).as("reg_exp")
  ).filter(col("reg_exp") =!= "").drop("reg_exp")
    .show(2)

//  println(MovieNameList.mkString("|"))







}