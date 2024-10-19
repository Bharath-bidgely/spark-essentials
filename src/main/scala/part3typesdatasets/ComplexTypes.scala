package part3typesdatasets

import org.apache.spark.sql.SparkSession

object ComplexTypes extends App {

  val spark = SparkSession.builder()
    .appName("ComplexTypes")
    .master("local")
    .getOrCreate()

  spark.sparkContext.setLogLevel("ERROR")

  //Dates



}
