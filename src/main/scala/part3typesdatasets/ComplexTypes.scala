package part3typesdatasets

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions._

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("ComplexTypes")
    .master("local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val moviesDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/movies.json")
  //moviesDF.show(2)

  //Dates
  val moviesWithReleaseDates = moviesDF.select(
    col("Title"),
    to_date(col("Release_Date"),"dd-MMM-yy").as("Release_Date_Formated"), //date_add,date_sub etc
    lit(curdate()).as("Today"),
    (datediff(col("Today"),col("Release_Date_Formated"))/365).as("Movie_average")
  )

  moviesWithReleaseDates.select(
    column("*")).
    filter(col("Release_Date_Formated").isNull)
    //.show()

  // List of date formats
  val dateFormats = List("dd-MMM-yy", "yyyy-MM-dd", "MM/dd/yyyy")

  // Coalesce to try each date format
  val moviesWithReleaseDates1 = moviesDF.select(
    col("Title"),
    coalesce(
      dateFormats.map(fmt => to_date(col("Release_Date"), fmt)): _*
    ).as("Release_Date_Formated"),
    lit(current_date()).as("Today"),
    (datediff(current_date(), coalesce(
      dateFormats.map(fmt => to_date(col("Release_Date"), fmt)): _*
    )) / 365).as("Movie_average")
  )

  //moviesWithReleaseDates1.select("*").filter(col("Release_Date_Formated").isNull).show()

  //struct
  moviesDF.select(
    col("Title"),
    struct(col("US_Gross"),col("Worldwide_Gross")).as("Profit")
  ).select(
    col("Title"),
    col("Profit").getField("US_GROSS").as("US_GROSS_PROFIT")
  ).show()






}
