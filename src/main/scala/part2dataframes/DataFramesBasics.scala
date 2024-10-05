package part2dataframes

import org.apache.spark.sql.{Row, SparkSession}
import org.apache.spark.sql.types.{LongType, StringType, StructField, StructType}

object DataFramesBasics extends App {

  // Create a SparkSession
  val spark = SparkSession.builder()
    .appName("DataFrames Basics")
    .config("spark.master", "local")
    .getOrCreate()

  println(spark.version)

  // Read a CSV file into a DataFrame
  private val firstDF = spark.read
    .format("json")
    .option("inferSchema", "true")
    .load("src/main/resources/data/cars.json")
    .cache()

  // Show the DataFrame
  //firstDF.show()
  firstDF.printSchema()

  // gets rows
  //firstDF.take(10).foreach(println)

  // spark types
  //val longType = LongType

  //schema
  var carsSchemas = StructType(
    Array(
      StructField("Name", StringType),
      StructField("Miles_per_Gallon", LongType),
      StructField("Cylinders", LongType),
      StructField("Displacement", LongType),
      StructField("Horsepower", LongType),
      StructField("Weight_in_lbs", LongType),
      StructField("Acceleration", LongType),
      StructField("Year", StringType),
      StructField("Origin", StringType)
    )
  )

  //carsSchemas.foreach(println)
  //carsSchemas.printTreeString()

  //obtain schema
  val carsDFSchema = firstDF.schema
  println(carsDFSchema)

  // Read a DF with your own schema
  val carsDFWithSchema = spark.read
    .format("json")
    .schema(carsDFSchema)
    //.schema(carsSchemas)
    .load("src/main/resources/data/cars.json")
    .cache()

  println(carsDFWithSchema.schema)

  //create a row
  val myRow = Row("abc", 21L, 8L, 307L, 130L, 3504L, 12L, "1970-01-01", "USA")
  println(myRow) // prints [abc,21,8,307,130,3504,12,1970-01-01,USA]

  val cars = Seq(
    ("chevrolet chevelle malibu",18.0,8L),
    ("buick skylark 320",15.0,8L),
    ("plymouth satellite",18.0,8L),
    ("amc rebel sst",16.0,8L),
    ("ford torino",17.0,8L),
    ("ford galaxie 500",15.0,8L),
    ("chevrolet impala",14.0,8L),
    ("plymouth fury iii",14.0,8L),
    ("pontiac catalina",14.0,8L),
    ("amc ambassador dpl",15.0,8L)
  )

  println("this is the cars list")
  val manualCarrsDF = spark.createDataFrame(cars) // schema auto-inferred
  manualCarrsDF.printSchema()

  //note: DF have schemas, Row do not

  //create DF's with implicts
  import spark.implicits._
  val manualCarsWithImplictsDF = cars.toDF("Name", "MPG", "Cylinders")
  manualCarsWithImplictsDF.printSchema()


  spark.stop()
}
