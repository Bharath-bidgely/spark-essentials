package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.types.{DateType, DoubleType, LongType, StringType, StructField, StructType}
import org.apache.spark.sql.types._
import org.json4s.DefaultFormats.dateFormat
import part2dataframes.DataFramesBasicExcercise.spark
import part2dataframes.DataFramesBasics.{carsDFWithSchema, spark}

object DataSources extends App {
  val spark = SparkSession.builder()
    .appName("Data sources")
    .config("spark.master", "local")
    .getOrCreate()

  println("Spark session created")

  var carsSchemas = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", DoubleType),
      StructField("Cylinders", LongType),
      StructField("Displacement", DoubleType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", DoubleType),
      //StructField("Year", StringType),
      StructField("Year", DateType),
      StructField("Origin", StringType)
    )
  )

  private val carsDF = spark.read
    .format("json")
    //.option("inferSchema", "true")
    .schema(carsSchemas)
    .option("mode", "failFast") // dropMalformed, permissive (Default)
    .option("path", "src/main/resources/data/cars.json")
    .load()
    //.load("src/main/resources/data/cars.json")
    .cache()

  //carsDF.show()
  //println(carsDF.schema)
  //moviesDF.printSchema()

  private val carsDFWithOptionMap = spark.read
    .format("json")
    .options(Map(
      "mode" -> "failFast",
      "path" -> "src/main/resources/data/cars.json",
      "inferSchema" -> "true"
    ))
    .load()
    .cache()

  //println(carsDFWithOptionMap.schema)

  //write a DF to the file system
  carsDF.write
    .format("json")
    .mode("overwrite")
    .option("path", "src/main/resources/data/cars_dupe.json")
    .save()

  //json flags
  private val carsDFWithOptionMap2 = spark.read
  //  .format("json")
    .option("schema","carsSchemas")
    .option("dateFormat","yyyy-MM-dd")
    .option("allowSingleQuotes","true")
    .option("compression","uncompressed") // bzip2, gzip, lz4, snappy, deflate
    .json("src/main/resources/data/cars.json")

  //carsDFWithOptionMap2.take(10).foreach(println)

  //csv flags
  private val stocksSchema = StructType(
    Array(
      StructField("symbol", StringType),
      StructField("date", DateType),
      StructField("price", DoubleType)
    )
  )

  private val stocksDF = spark.read
    //.format("csv")
    //.header(true)'
    .option("header","true")
    .schema(stocksSchema)
    .option("dateFormat","MMM d yyyy")
    .option("sep",",")
    .option("nullValue","")
    //.load("src/main/resources/data/stocks.csv")
    .csv("src/main/resources/data/stocks.csv")

  //stocksDF.take(10).foreach(println)

  //parquet
  carsDF.write
    //.format("parquet") //Default Parquet
    .mode("overwrite")
    .save("src/main/resources/data/cars.parquet")


  //text files
  //private val sampleTextDF = spark.read.text("src/main/resources/data/sampleTextFile.txt").show()
  private val sampleTextDF = spark.read
    //.format("text")
    .text("src/main/resources/data/sampleTextFile.txt")
    //.show()

  //reading from a remote DB
  private val employeesDF = spark.read
    .format("jdbc")
    .option("driver","org.postgresql.Driver")
    .option("url","jdbc:postgresql://localhost:5432/rtjvm")
    .option("user","docker")
    .option("password","docker")
    .option("dbtable","public.employees")
    .load()
    //.show()

  println("exercises starts here")

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema","true")
    .load("src/main/resources/data/movies.json")
    .cache()

  moviesDF.show()

  moviesDF.write
    .format("csv")
    .option("header","true")
    .option("delimiter","\t")
    .mode("overwrite")
    .save("src/main/resources/data/Adhoc/movies.csv")

  moviesDF.write
    .format("parquet")
    .mode("overwrite")
    .option("compression","snappy")
    .save("src/main/resources/data/Adhoc/movies.parquet")

  val jdbcOptions = Map(
    "driver" -> "org.postgresql.Driver",
    "url" -> "jdbc:postgresql://localhost:5432/rtjvm",
    "user" -> "docker",
    "password" -> "docker",
    "dbtable" -> "public.movies"
  )

  moviesDF.write
    .format("jdbc")
    .options(jdbcOptions)
    .mode("overwrite")
    .save()












}
