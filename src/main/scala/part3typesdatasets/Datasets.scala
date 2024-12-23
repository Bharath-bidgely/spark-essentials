package part3typesdatasets

import org.apache.spark.sql.{Dataset, SparkSession}

import java.util.Date
import scala.math.Ordering.Implicits.infixOrderingOps


object Datasets extends App {

  val spark = SparkSession.builder()
    .appName("Datasets")
    .master("local")
    .config("spark.sql.legacy.timeParserPolicy", "LEGACY")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  val numberDF = spark.read.options(Map("InferSchema" -> "true", "Header" -> "true")).csv("src/main/resources/data/numbers.csv")
  //numberDF.show(2, truncate = false)

  //numberDF.printSchema()

  import spark.implicits._

  val numberDS: Dataset[Int] = numberDF.as[Int]
  //numberDS.printSchema()
  //numberDS.filter(_ > 100).show(2)


  /**
   * root
   * |-- Acceleration: double (nullable = true)
   * |-- Cylinders: long (nullable = true)
   * |-- Displacement: double (nullable = true)
   * |-- Horsepower: long (nullable = true)
   * |-- Miles_per_Gallon: double (nullable = true)
   * |-- Name: string (nullable = true)
   * |-- Origin: string (nullable = true)
   * |-- Weight_in_lbs: long (nullable = true)
   * |-- Year: string (nullable = true)
   */

  case class Cars (
                  Name: String,
                  Miles_per_Gallon: Option[Double],
                  Acceleration: Option[Double],
                  Cylinders: Long,
                  Displacement: Option[Double],
                  Origin: String,
                  Weight_in_lbs: Long,
                  Year: String,
                  Horsepower: Option[Long]
                  )

  //to intake cars data where multiple column DF to be converted to DS
  val carsDF = spark.read.option("InferSchema","true").json("src/main/resources/data/cars.json")
  carsDF.show(2,truncate = false)

  val carsDS: Dataset[Cars] = carsDF.as[Cars]
  //we can use any of the scala collection easily here.
  carsDS.map(
    cars => cars.Name.toUpperCase()
  ).show(2)

  val carsCount = carsDS.count()
  println(carsCount)

  println(carsDS.filter(_.Horsepower.getOrElse(0L) > 140).count)



}
