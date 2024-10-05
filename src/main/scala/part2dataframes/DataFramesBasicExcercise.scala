package part2dataframes

import org.apache.spark.sql.SparkSession

object DataFramesBasicExcercise extends App {
  // Create a spark session
  val spark = SparkSession.builder()
    .appName("DataFrames Basic Excercise")
    .config("spark.master", "local")
    .getOrCreate()

  println("Spark session created")
  println(spark.version)

  // create manual df for smartphones
  val smartPhhones = Seq(
      ("samsung", "galaxy s20", 7,20)
      ,("samsung", "galaxy s20", 7,20)
      ,("samsung", "galaxy s20", 7,20)
      ,("samsung", "galaxy s20", 7,20)
  )

  //smartPhhones.foreach(println)

  import spark.implicits._
  val smartPhonesDF = smartPhhones.toDF("Manufacturer", "Model", "AndroidVersion", "ScreenSize")
  smartPhonesDF.printSchema()
  println(smartPhonesDF.schema)

  //create a DF from file example movies.json

  val moviesDF = spark.read
    .format("json")
    .option("inferSchema","true")
    .load("src/main/resources/data/movies.json")
    .cache()

  moviesDF.printSchema()
  println(moviesDF.count())

  val moviesDFSchema = moviesDF.schema

  val movieWithDFSchema = spark.read
    .format("json")
    .schema(moviesDFSchema)
    .load("src/main/resources/data/movies.json")
    .cache()

  println(movieWithDFSchema.schema)



}
