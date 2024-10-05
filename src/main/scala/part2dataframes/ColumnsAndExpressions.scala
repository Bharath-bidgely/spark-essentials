package part2dataframes

import org.apache.spark.sql.SparkSession
import org.apache.spark.sql.functions.{col, column, expr}

object ColumnsAndExpressions extends App {
  val spark = SparkSession.builder()
    .appName("ColumnAndExpressions")
    .master("local")
    .getOrCreate()

  println(spark.version)

  //both are correct.
  //val moviesDF = spark.read.format("json").option("inferSchema", "true").option("path","src/main/resources/data/cars.json").load()
  val carsDF = spark.read.option("inferSchema", "true").json("src/main/resources/data/cars.json")
  //moviesDF.show()

  //columns
  private val firstColumn = carsDF.col("Name")

  //selecting (projecting)
  private val carNamesDF = carsDF.select(firstColumn)
  //carNamesDF.show(2)

  import spark.implicits._

  //various select methods
  carsDF.select(
    carsDF.col("Name"),
    col("Acceleration"),
    column("Weight_in_lbs"),
    expr("Origin")
  )//.show(10)

  carsDF.select("Name","Year")
    //.show(2)

  //Expressions

  val Weight_in_lbs = carsDF.col("Weight_in_lbs")
  val Weight_in_kg = carsDF.col("Weight_in_lbs") / 2.2

  carsDF.select(
    col("Name"),
    col("Weight_in_lbs"),
    Weight_in_kg.as("Weight_in_kg"),
    expr("Weight_in_lbs / 2.2 ").as("Weight_in_kg_2")
  )
    //.show()

  //SelectExpr
  carsDF.selectExpr(
    "Name",
    "Weight_in_lbs",
    "Weight_in_lbs / 2.2 as Weight_in_kg"
  )
    //.show()

  //DF processing

  //Adding a column
  carsDF.withColumn("Weight_in_kg_3",col("Weight_in_lbs")/2.2)

  //renaming a column
  val carsDFRenamed = carsDF.withColumnRenamed("Weight_in_lbs","Weight in pounds")

  //careful with column names
  carsDFRenamed.selectExpr("`Weight in pounds`")

  //removing a column
  carsDFRenamed.drop("Cylinders","Displacement")

  //filtering
  val nonAmericanCars = carsDF.filter(col("Origin") !== "USA")
  val nonAmericanCarswhere = carsDF.where(col("Origin") !== "USA")
  val americanCars = carsDF.filter(col("Origin") === "USA")

  val americanPowerfulCars = carsDF.filter(col("Origin") === "USA").filter(col("Horsepower") > 150)
  val amercianPowerfulCars2 = carsDF.filter(col("Origin") === "USA" and col("Horsepower") > 150)
  val americanPowerfulCars3 = carsDF.filter("Origin = 'USA' and Horsepower > 150") // this is better to use.

  //union = adding more rows
  val moreCarsDF = spark.read.option("inferSchema","true").json("src/main/resources/data/more_cars.json")
  val allCarsDF = carsDF.union(moreCarsDF) //this will only work if the schema is same

  //distinct values
  val allCountriesDF = allCarsDF.select("Origin").distinct()
  allCountriesDF.show()

}
