package part2dataframes

import org.apache.spark.sql.SparkSession
import part2dataframes.Aggregations.spark
import org.apache.spark.sql.functions._

object Joins extends App {

  val spark = SparkSession.builder()
    .appName("Joins")
    .master("local")
    .getOrCreate()

  // Set log level to ERROR
  spark.sparkContext.setLogLevel("ERROR")

  // println(spark.conf.getAll)

  val guitarsDF = spark.read.option("infer_Schema","true").json("src/main/resources/data/guitars.json")
  //guitarsDF.show()

  val guitarPlayersDF = spark.read.option("infer_Schema","true").json("src/main/resources/data/guitarPlayers.json")
  //guitarPlayersDF.show()

  val bandsDF = spark.read.option("infer_Schema","true").json("src/main/resources/data/bands.json")
  //bandsDF.show()

  val joinCondition = guitarPlayersDF.col("band") === bandsDF.col("id")

  val guitarBandDF = guitarPlayersDF.join(bandsDF,joinCondition,"inner")
  guitarBandDF.show()
  //guitarBandDF.printSchema()

  //leftjoin
  //guitarPlayersDF.join(bandsDF,joinCondition,"left_outer").show()

  //rightjoin

  //fullouter
  //guitarPlayersDF.join(bandsDF,joinCondition,"outer").show()

  //leftsemi
  //guitarPlayersDF.join(bandsDF,joinCondition,"left_semi").show()

  //righsemi

  //antijoin - left_anti -- non equal joins
  //guitarPlayersDF.join(bandsDF,joinCondition,"left_anti").show()

  //guitarBandDF.select("id","band").show() //this should crash.
  //to handle the ambiguous columns option 1 is to rename

  val testDF = guitarPlayersDF.join(bandsDF.withColumnRenamed("id","band"),"band","inner")
  //testDF.printSchema()

  //Option-2 Drop the column
  guitarBandDF.drop(bandsDF.col("id"))

  //option 3 - rename and keep the data

  val bandsModDF = bandsDF.withColumnRenamed("id","bandid")

  if (guitarPlayersDF == null) {
    println("guitarPlayersDF is null")
  } else {
    println("guitarPlayersDF is not null")
  }

  if (bandsModDF == null) {
    println("bandsModDF is null")
  } else {
    println("bandsModDF is not null")
  }

  if (joinCondition2 == null) {
    println("joinCondition2 is null")
  } else {
    println("joinCondition2 is not null")
  }

  //rename the column and keep the data ( not a viable option )
  val joinCondition2 = guitarPlayersDF.col("band") === bandsModDF.col("bandid")
  guitarPlayersDF.join(bandsModDF,joinCondition2).show()

  //using complex types
  guitarPlayersDF.join(guitarsDF.withColumnRenamed("id","guitarid"), expr("array_contains(guitars,guitarid)")).show()

  /**
    * exercises
    *
    * max salary
    * never managers
    * job titles
    */



}
