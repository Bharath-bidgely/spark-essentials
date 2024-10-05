package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{coalesce, expr}
import org.apache.spark.sql.types.{DateType, StringType, StructField, StructType}

object ColumnsAndExpressions_Exercise extends App {

  //create spark session
  val spark = SparkSession.builder()
    .appName("ColumnsAndExpressions_Exercise")
    .master("local")
    .getOrCreate()

  // Set log level to WARN
  spark.sparkContext.setLogLevel("WARN")

  println(spark.version)

  val moviesDF = spark.read.option("inferSchema","true").json("src/main/resources/data/movies.json")
  //moviesDF.show(2)

  val carsSchema = StructType(
    Array(
      StructField ("Title", StringType),
      StructField ("Release_Date", DateType)
    )
  )

  val selectedColsCarsDF = moviesDF.selectExpr(
    "Title", "Release_Date"
  )//.show(2)

  val totalProfit = expr("US_Gross + Worldwide_Gross + coalesce(US_DVD_Sales,0)")
  val moreColumnsCarsDF = moviesDF.withColumn("Totalprofit",totalProfit)

  val moreColsDF = moviesDF.selectExpr(
    "Title", "Release_Date",
    "US_Gross + Worldwide_Gross + coalesce(US_DVD_Sales,0) as Totalprofit"
  )//.show(2)

  //moreColumnsCarsDF.show(2)

  moviesDF.filter("Major_Genre = 'Comedy' and IMDB_Rating > 6").show(2)




}
